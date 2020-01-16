use crate::{KvsEngine, KvsError, Result};
use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    fs::{self, File},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use crossbeam_skiplist::SkipMap;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

const MAX_FILE_SIZE: u64 = 1024 * 1024;
const COMPACTION_THRESHOLD: u64 = (MAX_FILE_SIZE as f64 * 0.6) as u64;

#[derive(Clone)]
pub struct KvStore {
    reader: KvsReader,
    writer: KvsWriter,
}

#[derive(Clone)]
struct KvsReader {
    dir: Arc<PathBuf>,
    keydir: Arc<SkipMap<String, CommandPos>>,
    readers: RefCell<BTreeMap<u64, BufReader<File>>>,
}

#[derive(Clone)]
struct KvsWriter {
    dir: Arc<PathBuf>,
    keydir: Arc<SkipMap<String, CommandPos>>,
    active_gen: Arc<Mutex<u64>>, // TODO: use atomics?
    writer: Arc<Mutex<BufWriter<File>>>,
    dead_bytes: Arc<Mutex<HashMap<u64, u64>>>,
}

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String, gen: u64 },
}

#[derive(Debug)]
struct CommandPos {
    gen: u64,
    pos: u64,
    len: u64,
}

impl KvsReader {
    fn get(&self, key: String) -> Result<Option<String>> {
        match self.keydir.get(&key) {
            Some(entry) => {
                let &CommandPos { gen, pos, len } = entry.value();
                let mut readers = self.readers.borrow_mut();
                let reader = readers
                    .entry(gen)
                    .or_insert(BufReader::new(File::open(get_log_path(&self.dir, gen))?));
                reader.seek(SeekFrom::Start(pos))?;
                match serde_json::from_reader(reader.take(len))? {
                    Command::Set { value, .. } => Ok(Some(value)),
                    rm => panic!("Wrong remove command: {:?}", rm),
                }
            }
            None => Ok(None),
        }
    }
}

impl KvsWriter {
    fn use_next_gen(&self) -> Result<()> {
        let mut active_gen = self.active_gen.lock().unwrap();
        *active_gen += 1;
        let path = get_log_path(&self.dir, *active_gen);
        *self.writer.lock().unwrap() = BufWriter::new(
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)?,
        );
        Ok(())
    }

    fn write_command(&self, cmd: Command) -> Result<(u64, u64)> {
        let mut writer = self.writer.lock().unwrap();
        let len = writer.get_ref().metadata()?.len();
        if len >= MAX_FILE_SIZE {
            self.use_next_gen()?;
            // self.compact(*self.active_gen.lock().unwrap() - 1)?;
        }
        let pos = writer.stream_position()?;
        serde_json::to_writer(&mut *writer, &cmd)?;
        let len = writer.stream_position()? - pos;
        writer.flush()?;
        Ok((pos, len))
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        let (pos, len) = self.write_command(Command::Set {
            key: key.clone(),
            value,
        })?;
        let old = self.keydir.remove(&key);
        self.keydir.insert(
            key,
            CommandPos {
                gen: *self.active_gen.lock().unwrap(),
                pos,
                len,
            },
        );
        if let Some(old) = old {
            let old = old.value();
            *self.dead_bytes.lock().unwrap().entry(old.gen).or_insert(0) += old.len;
            // self.compact(old.gen)?;
        }
        Ok(())
    }

    fn remove(&self, key: String) -> Result<()> {
        match self.keydir.remove(&key) {
            Some(old) => {
                let old = old.value();
                let (_, rm_len) = self.write_command(Command::Remove { key, gen: old.gen })?;
                let mut dead_bytes = self.dead_bytes.lock().unwrap();
                let active_gen = *self.active_gen.lock().unwrap();
                *dead_bytes.entry(old.gen).or_insert(0) += old.len;
                if old.gen == active_gen {
                    *dead_bytes.entry(active_gen).or_insert(0) += rm_len;
                }
                // self.compact(old.gen)?;
                Ok(())
            }
            None => Err(KvsError::KeyNotFound),
        }
    }
}

fn get_log_path(dir: &PathBuf, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

impl KvStore {
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = Arc::new(dir.into());

        let mut logs = Vec::new();
        for entry in fs::read_dir(&*dir)? {
            let path = entry?.path();
            if path.is_file() && path.extension() == Some("log".as_ref()) {
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
        let mut writer = BufWriter::new(
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(get_log_path(&dir, active_gen))?,
        );
        // For consistent behaviour, we explicitly seek to the end of file
        // (otherwise, it might be done only on the first write()).
        // -- copied from cpython's Lib/_pyio.py
        writer.seek(SeekFrom::End(0))?;

        let mut readers = BTreeMap::new();
        for gen in logs {
            readers.insert(gen, BufReader::new(File::open(get_log_path(&dir, gen))?));
        }

        let (keydir, dead_bytes) =
            Self::read_hints(&dir).or_else(|_| Self::replay_logs(&mut readers))?;

        let keydir = Arc::new(keydir);

        Ok(KvStore {
            reader: KvsReader {
                dir: Arc::clone(&dir),
                keydir: Arc::clone(&keydir),
                readers: RefCell::new(readers),
            },
            writer: KvsWriter {
                dir: Arc::clone(&dir),
                keydir: Arc::clone(&keydir),
                active_gen: Arc::new(Mutex::new(active_gen)),
                writer: Arc::new(Mutex::new(writer)),
                dead_bytes: Arc::new(Mutex::new(dead_bytes)),
            },
        })
    }

    fn read_hints(dir: &PathBuf) -> Result<(SkipMap<String, CommandPos>, HashMap<u64, u64>)> {
        // let reader = BufReader::new(File::open(dir.join("keydir.hint"))?);
        // let keydir = serde_json::from_reader(reader)?;
        // let reader = BufReader::new(File::open(dir.join("dead_bytes.hint"))?);
        // let dead_bytes = serde_json::from_reader(reader)?;
        // Ok((keydir, dead_bytes))
        Err(KvsError::KeyNotFound) // TODO: SkipMap is not serializable
    }

    fn replay_logs(
        readers: &mut BTreeMap<u64, BufReader<File>>,
    ) -> Result<(SkipMap<String, CommandPos>, HashMap<u64, u64>)> {
        let keydir = SkipMap::<String, CommandPos>::new();
        let mut dead_bytes = HashMap::new();
        for (&gen, reader) in readers.iter_mut() {
            let mut pos = 0;
            let mut stream = Deserializer::from_reader(reader).into_iter();
            while let Some(cmd) = stream.next() {
                let new_pos = stream.byte_offset() as u64;
                let len = new_pos - pos;
                match cmd? {
                    Command::Set { key, .. } => {
                        if let Some(old) = keydir.get(&key) {
                            *dead_bytes.entry(old.value().gen).or_insert(0) += old.value().len;
                        }
                        keydir.insert(key, CommandPos { gen, pos, len });
                    }
                    Command::Remove { key, .. } => match keydir.remove(&key) {
                        Some(old) => {
                            *dead_bytes.entry(old.value().gen).or_insert(0) += old.value().len
                        }
                        None => *dead_bytes.entry(gen).or_insert(0) += len,
                    },
                }
                pos = new_pos;
            }
        }
        Ok((keydir, dead_bytes))
    }
}

impl KvsEngine for KvStore {
    fn get(&self, key: String) -> Result<Option<String>> {
        self.reader.get(key)
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        self.writer.set(key, value)
    }

    fn remove(&self, key: String) -> Result<()> {
        self.writer.remove(key)
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
