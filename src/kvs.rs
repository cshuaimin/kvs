use std::collections::HashMap;

use async_std::fs::{self, File, OpenOptions};
use async_std::io::{self, SeekFrom};
use async_std::path::PathBuf;
use async_std::prelude::*;
use async_std::sync::{Arc, Mutex};
use async_std::task;

use serde::{Deserialize, Serialize};

use crate::{KvsError, Result, SkipMap};

const MAX_FILE_SIZE: u64 = 1024;
const COMPACTION_THRESHOLD: u64 = (MAX_FILE_SIZE as f64 * 0.6) as u64;

#[derive(Clone)]
pub struct KvStore {
    reader: KvsReader,
    writer: Arc<Mutex<KvsWriter>>,
}

#[derive(Clone)]
struct KvsReader {
    dir: Arc<PathBuf>,
    keydir: Arc<SkipMap<Vec<u8>, LogPos>>,
    readers: Arc<SkipMap<u64, File>>,
    rio: rio::Rio,
}

struct KvsWriter {
    dir: Arc<PathBuf>,
    keydir: Arc<SkipMap<Vec<u8>, LogPos>>,
    readers: Arc<SkipMap<u64, File>>,
    rio: rio::Rio,
    active_gen: u64,
    writer: File,
    writer_pos: u64,
    dead_bytes: HashMap<u64, u64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LogPos {
    gen: u64,
    pos: u64,
    len: u64,
}

impl KvStore {
    pub async fn open(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = Arc::new(dir.into());
        let mut active_gen = 0;
        let readers = Arc::new(SkipMap::new());
        let mut files = fs::read_dir(&*dir).await?;
        while let Some(file) = files.next().await {
            let path = file?.path();
            if path.is_file().await && path.extension() == Some("log".as_ref()) {
                let gen: u64 = path.file_stem().unwrap().to_str().unwrap().parse()?;
                active_gen = active_gen.max(gen);
                readers.insert(gen, File::open(path).await?);
            }
        }
        let mut writer = OpenOptions::new()
            .create(true)
            .write(true)
            .open(get_log_path(&dir, active_gen))
            .await?;
        let writer_pos = writer.seek(SeekFrom::End(0)).await?;
        if readers.is_empty() {
            readers.insert(0, File::open(get_log_path(&dir, 0)).await?);
        }

        let rio = rio::new()?;
        let (keydir, dead_bytes) = match File::open(get_keydir_path(&dir)).await {
            Ok(file) => {
                let buffer = vec![0u8; file.metadata().await?.len() as usize];
                rio.read_at(&file, &buffer, 0).await?;
                bincode::deserialize(&buffer).unwrap()
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Default::default(),
            Err(e) => return Err(e.into()),
        };
        let keydir = Arc::new(keydir);

        Ok(KvStore {
            reader: KvsReader {
                dir: Arc::clone(&dir),
                keydir: Arc::clone(&keydir),
                readers: Arc::clone(&readers),
                rio: rio.clone(),
            },
            writer: Arc::new(Mutex::new(KvsWriter {
                dir,
                keydir,
                rio,
                active_gen,
                readers,
                writer,
                writer_pos,
                dead_bytes,
            })),
        })
    }

    pub async fn get<K>(&self, key: K) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        self.reader.get(key.as_ref()).await
    }

    pub async fn set<K, V>(&self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let mut writer = self.writer.lock().await;
        if let Some(gen) = writer.set(key.as_ref(), value.as_ref()).await? {
            self.compact(gen, &mut writer).await?;
        }
        Ok(())
    }

    pub async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let mut writer = self.writer.lock().await;
        if let Some(gen) = writer.remove(key.as_ref()).await? {
            self.compact(gen, &mut writer).await?;
        }
        Ok(())
    }

    async fn compact(&self, gen: u64, writer: &mut KvsWriter) -> Result<()> {
        for entry in self.reader.keydir.iter().filter(|x| x.value().gen == gen) {
            let key = entry.key();
            let value = self.reader.get(key).await?.unwrap();
            writer.set(key, &value).await?;
        }
        writer.dead_bytes.remove(&gen);
        writer.readers.remove(&gen);
        fs::remove_file(get_log_path(&writer.dir, gen)).await?;
        Ok(())
    }
}

impl KvsReader {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.keydir.get(key) {
            Some(entry) => {
                let &LogPos { gen, pos, len } = entry.value();
                let file = self.readers.get(&gen).unwrap();
                let buffer = vec![0u8; len as usize];
                self.rio.read_at(file.value(), &buffer, pos).await?;
                Ok(Some(buffer))
            }
            None => Ok(None),
        }
    }
}

impl KvsWriter {
    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<Option<u64>> {
        let res = self.remove(key).await.unwrap_or(None);
        if self.writer_pos >= MAX_FILE_SIZE {
            self.use_next_gen().await?;
        }
        self.rio
            .write_at(&self.writer, &value, self.writer_pos)
            .await?;
        self.keydir.insert(
            key.to_vec(),
            LogPos {
                gen: self.active_gen,
                pos: self.writer_pos,
                len: value.len() as u64,
            },
        );
        self.writer_pos += value.len() as u64;
        Ok(res)
    }

    async fn remove(&mut self, key: &[u8]) -> Result<Option<u64>> {
        match self.keydir.remove(key) {
            Some(old) => {
                let old = old.value();
                let dead = self.dead_bytes.entry(old.gen).or_insert(0);
                *dead += old.len;
                if *dead >= COMPACTION_THRESHOLD && old.gen != self.active_gen {
                    Ok(Some(old.gen))
                } else {
                    Ok(None)
                }
            }
            None => Err(KvsError::KeyNotFound),
        }
    }

    async fn use_next_gen(&mut self) -> Result<()> {
        self.active_gen += 1;
        let path = get_log_path(&self.dir, self.active_gen);
        self.writer = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)
            .await?;
        self.writer_pos = 0;
        self.readers
            .insert(self.active_gen, File::open(&path).await?);
        Ok(())
    }
}

impl Drop for KvsWriter {
    fn drop(&mut self) {
        let _ = task::block_on(async {
            let file = File::create(get_keydir_path(&self.dir)).await?;
            let data = bincode::serialize(&(&*self.keydir, &self.dead_bytes)).unwrap();
            self.rio.write_at(&file, &data, 0).await?;
            Result::<()>::Ok(())
        });
    }
}

fn get_log_path(dir: &PathBuf, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

fn get_keydir_path(dir: &PathBuf) -> PathBuf {
    dir.join("keydir")
}
