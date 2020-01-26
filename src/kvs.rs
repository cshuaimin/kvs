use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use async_std::fs::{self, File, OpenOptions};
use async_std::io::{self, SeekFrom};
use async_std::path::PathBuf;
use async_std::prelude::*;
use async_std::sync::RwLock;
use async_std::task;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::{KvsError, Result};

const MAX_FILE_SIZE: u64 = 1024 * 1024;
// const COMPACTION_THRESHOLD: u64 = (MAX_FILE_SIZE as f64 * 0.6) as u64;

#[derive(Debug)]
pub struct KvStore {
    dir: PathBuf,
    keydir: DashMap<Vec<u8>, LogPos>,
    rio: rio::Rio,
    active_gen: AtomicU64,
    readers: BTreeMap<u64, File>,
    writer: RwLock<File>,
    writer_pos: AtomicU64,
    dead_bytes: DashMap<u64, u64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LogPos {
    gen: u64,
    pos: u64,
    len: u64,
}

impl KvStore {
    pub async fn open(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        let mut logs = Vec::new();
        let mut files = fs::read_dir(&dir).await?;
        while let Some(file) = files.next().await {
            let path = file?.path();
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
        let mut writer = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(get_log_path(&dir, active_gen))
            .await?;
        let active_gen = AtomicU64::new(active_gen);

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
        let (keydir, dead_bytes) = match File::open(dir.join("save")).await {
            Ok(file) => {
                let buffer = vec![0u8; file.metadata().await?.len() as usize];
                rio.read_at(&file, &buffer, 0)?.await?;
                bincode::deserialize(&buffer).unwrap()
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Default::default(),
            Err(e) => return Err(e.into()),
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

    async fn use_next_gen(&self) -> Result<()> {
        let mut writer = self.writer.write().await;
        let active_gen = 1 + self.active_gen.fetch_add(1, Ordering::SeqCst);
        let path = get_log_path(&self.dir, active_gen);
        *writer = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        self.writer_pos.store(0, Ordering::SeqCst);
        Ok(())
    }

    async fn write_log(&self, value: &[u8], key: &[u8]) -> Result<LogPos> {
        if self.writer_pos.load(Ordering::SeqCst) >= MAX_FILE_SIZE {
            self.use_next_gen().await?;
        }

        let pos = self
            .writer_pos
            .fetch_add(value.len() as u64, Ordering::SeqCst);
        let writer = self.writer.read().await;
        self.write(&*writer, &value, pos, key).await?;
        // self.rio.write_at(&*writer, &value, pos)?.await?;

        Ok(LogPos {
            gen: self.active_gen.load(Ordering::SeqCst),
            pos,
            len: value.len() as u64,
        })
    }

    async fn write(&self, writer: &File, value: &&[u8], pos: u64, key: &[u8]) -> Result<()> {
        let key = String::from_utf8(key.to_vec()).unwrap();
        println!(
            "{}: writing {} to {}",
            key,
            String::from_utf8(value.to_vec()).unwrap(),
            pos
        );
        self.rio.write_at(&*writer, &value, pos)?.await?;

        let buf = vec![0u8; value.len()];
        self.rio.read_at(&*writer, &buf, pos)?.await?;
        println!(
            "{}: value should be {}, read {}",
            key,
            String::from_utf8(value.to_vec()).unwrap(),
            String::from_utf8(buf).unwrap(),
        );
        Ok(())
    }

    pub async fn get<K>(&self, key: K) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        match self.keydir.get(key.as_ref()) {
            Some(entry) => {
                let &LogPos { gen, pos, len } = entry.value();
                let file = self.readers.get(&gen).unwrap();
                let buffer = vec![0u8; len as usize];
                self.rio.read_at(file, &buffer, pos)?.await?;
                Ok(Some(buffer))
            }
            None => Ok(None),
        }
    }

    pub async fn set<K, V>(&self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref().to_vec();
        let value = value.as_ref();
        let log_pos = self.write_log(value, &key).await?;
        if let Some(old) = self.keydir.insert(key, log_pos) {
            *self.dead_bytes.entry(old.gen).or_insert(0) += old.len;
        }
        Ok(())
    }

    pub async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let key = key.as_ref();
        match self.keydir.remove(key) {
            Some((_, old)) => {
                *self.dead_bytes.entry(old.gen).or_insert(0) += old.len;
                Ok(())
            }
            None => Err(KvsError::KeyNotFound),
        }
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        async fn save(this: &KvStore) -> Result<()> {
            let file = File::create(this.dir.join("save")).await?;
            let data = bincode::serialize(&(&this.keydir, &this.dead_bytes)).unwrap();
            this.rio.write_at(&file, &data, 0)?.await?;
            Ok(())
        }
        let _ = task::block_on(save(self));
    }
}

fn get_log_path(dir: &PathBuf, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}
