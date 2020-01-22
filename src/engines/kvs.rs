use crate::{KvsEngine, KvsError, Result};
use std::collections::BTreeMap;
use std::convert::TryInto;
use std::mem;
use std::sync::atomic::{AtomicU64, Ordering};

use async_std::fs::{self, File, OpenOptions};
use async_std::io::SeekFrom;
use async_std::path::PathBuf;
use async_std::prelude::*;
use async_std::sync::RwLock;

use dashmap::DashMap;

const MAX_FILE_SIZE: u64 = 1024 * 1024;
const COMPACTION_THRESHOLD: u64 = (MAX_FILE_SIZE as f64 * 0.6) as u64;

#[derive(Debug)]
pub struct KvStore {
    dir: PathBuf,
    keydir: DashMap<Vec<u8>, LogPos>,
    rio: rio::Rio,
    active_gen: u64,
    readers: BTreeMap<u64, File>,
    writer: RwLock<File>,
    writer_pos: AtomicU64,
    dead_bytes: DashMap<u64, u64>,
}

#[derive(Debug)]
enum Log {
    Set { key: Vec<u8>, value: Vec<u8> },
    Remove { key: Vec<u8> },
}

#[derive(Debug)]
struct LogPos {
    gen: u64,
    pos: u64,
    len: u64,
}

impl KvStore {
    pub async fn open(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        let mut logs = Vec::new();
        let entries = fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next().await {
            let path = entry?.path();
            if path.is_file().await && path.extension() == Some("log".as_ref()) {
                let gen: u64 = path.file_stem().unwrap().to_str().unwrap().parse()?;
                logs.push(gen);
            }
        }
        logs.sort_unstable();
        if logs.is_empty() {
            logs.push(0);
        }

        // Active file is used for both writing and reading.
        // Remember to flush the writer after writing to keep it in sync with the reader.
        let active_gen = *logs.last().unwrap();
        let writer = OpenOptions::new()
            .create(true)
            .append(true)
            .open(get_log_path(&dir, active_gen))
            .await?;
        // for consistent behaviour, we explicitly seek to the end of file
        // (otherwise, it might be done only on the first write()).
        // -- copied from cpython's lib/_pyio.py
        let writer_pos = AtomicU64::new(writer.seek(SeekFrom::End(0)).await?);
        let writer = RwLock::new(writer);

        let mut readers = BTreeMap::new();
        for gen in logs {
            readers.insert(gen, File::open(get_log_path(&dir, gen)).await?);
        }
        let rio = rio::new().expect("error creating rio");
        let (keydir, dead_bytes) = match Self::read_hints(&dir).await {
            Ok(r) => r,
            Err(_) => Self::replay_logs(&rio, &mut readers).await?,
        };

        Ok(KvStore {
            dir,
            keydir,
            rio,
            active_gen,
            readers,
            writer,
            writer_pos,
            dead_bytes,
        })
    }

    async fn read_hints(dir: &PathBuf) -> Result<(DashMap<Vec<u8>, LogPos>, DashMap<u64, u64>)> {
        // let reader = BufReader::new(File::open(dir.join("keydir.hint"))?);
        // let keydir = serde_json::from_reader(reader)?;
        // let reader = BufReader::new(File::open(dir.join("dead_bytes.hint"))?);
        // let dead_bytes = serde_json::from_reader(reader)?;
        // Ok((keydir, dead_bytes))
        Err(KvsError::KeyNotFound) // TODO: SkipMap is not serializable
    }

    async fn replay_logs(
        rio: &rio::Rio,
        readers: &mut BTreeMap<u64, File>,
    ) -> Result<(DashMap<Vec<u8>, LogPos>, DashMap<u64, u64>)> {
        let keydir = DashMap::<Vec<u8>, LogPos>::new();
        let mut dead_bytes = DashMap::with_capacity(readers.len());

        for (&gen, file) in readers.iter_mut() {
            let mut pos = 0;
            let (log, len) = Log::read(rio, file, pos).await?;
            match log {
                Log::Set { key, value } => {
                    if let Some(old) = keydir.get(&key) {
                        *dead_bytes.entry(old.value().gen).or_insert(0) += old.value().len;
                    }
                    keydir.insert(key, LogPos { gen, pos, len });
                }
                Log::Remove { key } => match keydir.remove(&key) {
                    Some((_, old_value)) => {
                        *dead_bytes.entry(old_value.gen).or_insert(0) += old_value.len
                    }
                    None => panic!(), // *dead_bytes.entry(gen).or_insert(0) += len,
                },
            }
            pos += len;
        }
        Ok((keydir, dead_bytes))
    }

    async fn use_next_gen(&mut self) -> Result<()> {
        let guard = self.writer.write().await;
        self.active_gen += 1;
        let path = get_log_path(&self.dir, self.active_gen);
        *guard = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        self.writer_pos.store(0, Ordering::SeqCst);
        Ok(())
    }

    async fn write_log(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<LogPos> {
        if self.writer_pos.load(Ordering::SeqCst) >= MAX_FILE_SIZE {
            self.use_next_gen().await?;
        }

        let len = (1                    // set or remove?
            + mem::size_of::<u64>()     // key size
            + key.len()                 // key
            + if value.is_some() {
                mem::size_of::<u64>()   // value size
                + value.unwrap().len()  // value
            } else {
                0
            }) as u64;
        let pos = self.writer_pos.fetch_add(len, Ordering::SeqCst);

        let mut start = pos;
        let writer = self.writer.read().await;
        macro_rules! write {
            ($data:expr) => {{
                let data = $data;
                let result = self.rio.write_at(&*writer, data, start);
                start += data.len() as u64;
                result
            };};
        }

        match value {
            Some(value) => {
                write!(&[1u8])?.await;
                write!(&key.len().to_be_bytes())?.await;
                write!(&value.len().to_be_bytes())?.await;
                write!(key)?.await;
                write!(value)?.await;
            }
            None => {
                write!(&[0u8])?.await;
                write!(key.len().to_be_bytes())?.await;
                write!(key)?.await;
            }
        }
        Ok(LogPos {
            gen: self.active_gen,
            pos,
            len,
        })
    }

    async fn get<K>(&mut self, key: K) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        match self.keydir.get(key.as_ref()) {
            Some(entry) => {
                let LogPos { gen, pos, .. } = entry.value();
                let file = self.readers.get(gen).unwrap();
                let (log, _) = Log::read(&self.rio, file, *pos).await?;
                match log {
                    Log::Set { value, .. } => Ok(Some(value)),
                    rm => panic!(format!("wrong remove log: {:?}", rm)),
                }
            }
            None => Ok(None),
        }
    }

    async fn set<K, V>(&mut self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref().to_vec();
        let value = value.as_ref();
        let log_pos = self.write_log(&key, Some(value)).await?;
        if let Some(old) = self.keydir.insert(key, log_pos) {
            *self.dead_bytes.entry(old.gen).or_insert(0) += old.len;
        }
        Ok(())
    }

    async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let key = key.as_ref();
        match self.keydir.remove(key) {
            Some((_, old)) => {
                let rm = self.write_log(key, None).await?;
                *self.dead_bytes.entry(old.gen).or_insert(0) += old.len;
                if old.gen != rm.gen {
                    *self.dead_bytes.entry(rm.gen).or_insert(0) += rm.len;
                }
                Ok(())
            }
            None => Err(KvsError::KeyNotFound),
        }
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        // let writer = BufWriter::new(File::create(self.dir.join("keydir.hint")).unwrap());
        // serde_json::to_writer(writer, &*self.keydir.lock().unwrap()).unwrap();
        // let writer = BufWriter::new(File::create(self.dir.join("dead_bytes.hint")).unwrap());
        // serde_json::to_writer(writer, &*self.dead_bytes.lock().unwrap()).unwrap();
    }
}

impl Log {
    async fn read(rio: &rio::Rio, file: &File, pos: u64) -> Result<(Log, u64)> {
        let buffer = vec![0u8; 1 + mem::size_of::<usize>() * 2];
        let read = rio.read_at(file, &buffer, pos)?.await?;
        match buffer[0] {
            0 => {
                let (key_len_bytes, value_len_bytes) =
                    buffer[1..].split_at(mem::size_of::<usize>());
                let key_len = usize::from_be_bytes(key_len_bytes.try_into().unwrap());
                let value_len = usize::from_be_bytes(value_len_bytes.try_into().unwrap());
                let key = vec![0u8; key_len];
                let value = vec![0u8; value_len];
                rio.read_at(file, &key, pos + 1 + mem::size_of::<usize>() as u64)?
                    .await;
                rio.read_at(file, &value, pos + 1 + mem::size_of::<usize>() as u64 * 2)?
                    .await;
                Ok((
                    Log::Set { key, value },
                    (1 + mem::size_of::<usize>() * 2 + key_len + value_len) as u64,
                ))
            }
            1 => {
                let key_len_bytes = &buffer[1..mem::size_of::<usize>()];
                let key_len = usize::from_be_bytes(key_len_bytes.try_into().unwrap());
                let key = vec![0u8; key_len];
                rio.read_at(file, &key, pos + 1 + mem::size_of::<usize>() as u64)?
                    .await;
                Ok((
                    Log::Remove { key },
                    (1 + mem::size_of::<usize>() + key_len) as u64,
                ))
            }
            _ => panic!(),
        }
    }
}

fn get_log_path(dir: &PathBuf, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}
