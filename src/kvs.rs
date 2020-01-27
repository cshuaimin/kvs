use std::sync::atomic::{AtomicU64, Ordering};

use async_std::fs::{self, File, OpenOptions};
use async_std::io::{self, SeekFrom};
use async_std::path::PathBuf;
use async_std::prelude::*;
use async_std::sync::{channel, Arc, Receiver, RwLock, Sender};
use async_std::task;

use serde::{Deserialize, Serialize};

use crate::{KvsError, Result, SkipMap};

const MAX_FILE_SIZE: u64 = 1000;
const COMPACTION_THRESHOLD: u64 = (MAX_FILE_SIZE as f64 * 0.6) as u64;

#[derive(Debug, Clone)]
pub struct KvStore {
    dir: Arc<PathBuf>,
    keydir: Arc<SkipMap<Vec<u8>, LogPos>>,
    rio: Arc<rio::Rio>,
    active_gen: Arc<AtomicU64>,
    readers: Arc<SkipMap<u64, File>>,
    writer: Arc<RwLock<File>>,
    writer_pos: Arc<AtomicU64>,
    dead_bytes: Arc<SkipMap<u64, u64>>,
    compact_sender: Sender<u64>,
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
        let mut logs = Vec::new();
        let mut files = fs::read_dir(&*dir).await?;
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

        let active_gen = *logs.last().unwrap();
        let mut writer = OpenOptions::new()
            .create(true)
            .write(true)
            .open(get_log_path(&dir, active_gen))
            .await?;
        let active_gen = Arc::new(AtomicU64::new(active_gen));
        let writer_pos = Arc::new(AtomicU64::new(writer.seek(SeekFrom::End(0)).await?));
        let writer = Arc::new(RwLock::new(writer));

        let readers = Arc::new(SkipMap::new());
        for gen in logs {
            readers.insert(gen, File::open(get_log_path(&dir, gen)).await?);
        }
        let rio = Arc::new(rio::new()?);
        let (keydir, dead_bytes) = match File::open(get_keydir_path(&dir)).await {
            Ok(file) => {
                let buffer = vec![0u8; file.metadata().await?.len() as usize];
                rio.read_at(&file, &buffer, 0)?.await?;
                let (keydir, dead_bytes) = bincode::deserialize(&buffer).unwrap();
                (Arc::new(keydir), Arc::new(dead_bytes))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Default::default(),
            Err(e) => return Err(e.into()),
        };

        let (compact_sender, compact_receiver) = channel(10);
        let kvs = KvStore {
            dir,
            keydir,
            rio,
            active_gen,
            readers,
            writer,
            writer_pos,
            dead_bytes,
            compact_sender,
        };
        let clone = kvs.clone();
        task::spawn(async move {
            clone.compact(compact_receiver).await.unwrap();
        });
        Ok(kvs)
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
                self.rio.read_at(file.value(), &buffer, pos)?.await?;
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
        if let Some(old) = self.keydir.get(&key) {
            let old = old.value();
            let dead_bytes = match self.dead_bytes.get(&old.gen) {
                Some(entry) => entry.value() + old.len,
                None => old.len,
            };
            self.dead_bytes.insert(old.gen, dead_bytes);
            if dead_bytes >= COMPACTION_THRESHOLD {
                self.compact_sender.send(old.gen).await;
            }
        }
        let log_pos = self.write_log(value).await?;
        let _g = self.writer.write().await;
        self.keydir.insert(key, log_pos);
        Ok(())
    }

    pub async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let key = key.as_ref();
        match self.keydir.remove(key) {
            Some(old) => {
                let old = old.value();
                let dead_bytes = match self.dead_bytes.get(&old.gen) {
                    Some(entry) => entry.value() + old.len,
                    None => old.len,
                };
                self.dead_bytes.insert(old.gen, dead_bytes);
                if dead_bytes >= COMPACTION_THRESHOLD {
                    self.compact_sender.send(old.gen).await;
                }
                Ok(())
            }
            None => Err(KvsError::KeyNotFound),
        }
    }

    async fn write_log(&self, value: &[u8]) -> Result<LogPos> {
        if self.writer_pos.load(Ordering::SeqCst) >= MAX_FILE_SIZE {
            self.use_next_gen().await?;
        }

        let writer = self.writer.read().await;
        let pos = self
            .writer_pos
            .fetch_add(value.len() as u64, Ordering::SeqCst);
        self.rio.write_at(&*writer, &value, pos)?.await?;

        Ok(LogPos {
            gen: self.active_gen.load(Ordering::SeqCst),
            pos,
            len: value.len() as u64,
        })
    }

    async fn use_next_gen(&self) -> Result<()> {
        let mut writer = self.writer.write().await;
        if self.writer_pos.load(Ordering::SeqCst) < COMPACTION_THRESHOLD {
            println!("UNG ignored");
            return Ok(());
        }
        let gen = 1 + self.active_gen.fetch_add(1, Ordering::SeqCst);
        let path = get_log_path(&self.dir, gen);
        *writer = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)
            .await?;
        self.writer_pos.store(0, Ordering::SeqCst);
        self.readers.insert(gen, File::open(&path).await?);
        Ok(())
    }

    async fn compact(&self, r: Receiver<u64>) -> Result<()> {
        while let Some(gen) = r.recv().await {
            if self.active_gen.load(Ordering::SeqCst) == gen
                || match self.dead_bytes.get(&gen) {
                    Some(entry) => *entry.value() < COMPACTION_THRESHOLD,
                    None => true,
                }
            {
                println!("Gen {} ignored", gen);
                continue;
            }

            if self.writer_pos.load(Ordering::SeqCst) >= MAX_FILE_SIZE {
                self.use_next_gen().await?;
            }
            println!("Compacting generation {}", gen);
            self.dead_bytes.remove(&gen);
            for entry in self.keydir.iter().filter(|x| x.value().gen == gen) {
                // if entry.is_removed() {
                //     println!("NPNPNNPPN");
                //     continue;
                // }
                let key = entry.key();
                let &LogPos { gen, pos, len } = entry.value();
                let file = self.readers.get(&gen).unwrap();
                let buffer = vec![0u8; len as usize];
                self.rio.read_at(file.value(), &buffer, pos)?.await?;
                let pos = self
                    .writer_pos
                    .fetch_add(buffer.len() as u64, Ordering::SeqCst);
                let writer = self.writer.read().await;
                self.rio.write_at(&*writer, &buffer, pos)?.await?;
                drop(writer);

                let log_pos = LogPos {
                    gen: self.active_gen.load(Ordering::SeqCst),
                    pos,
                    len: buffer.len() as u64,
                };
                let _g = self.writer.write().await;
                if !entry.is_removed() {
                    self.keydir.insert(key.to_vec(), log_pos);
                }
                drop(_g);
            }
            self.dead_bytes.remove(&gen);
            self.readers.remove(&gen);
            fs::remove_file(get_log_path(&self.dir, gen)).await?;
            println!("Generation {} compacted", gen);
        }
        eprintln!("Exiting compaction task");
        Ok(())
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        let _ = task::block_on(async {
            let file = File::create(get_keydir_path(&self.dir)).await?;
            let data = bincode::serialize(&(&*self.keydir, &*self.dead_bytes)).unwrap();
            self.rio.write_at(&file, &data, 0)?.await?;
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
