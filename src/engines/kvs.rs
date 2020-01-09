use crate::{KvsEngine, KvsError, Result};
use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String, gen: u64 },
}

#[derive(Serialize, Deserialize, Debug)]
struct CommandPos {
    gen: u64,
    pos: u64,
    len: u64,
}

const MAX_FILE_SIZE: u64 = 1024 * 1024 * 1024;
const COMPACTION_THRESHOLD: u64 = (MAX_FILE_SIZE as f64 * 0.6) as u64;

pub struct KvStore {
    dir: PathBuf,
    active_gen: u64,
    // Reading order is important
    readers: BTreeMap<u64, BufReader<File>>,
    writer: BufWriter<File>,
    keydir: HashMap<String, CommandPos>,
    pub dead_bytes: HashMap<u64, u64>,
}

fn get_log_path(dir: &PathBuf, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

impl KvStore {
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();

        let mut logs = Vec::new();
        for entry in fs::read_dir(&dir)? {
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
        Ok(KvStore {
            dir,
            readers,
            active_gen,
            writer,
            keydir,
            dead_bytes,
        })
    }

    fn read_hints(dir: &PathBuf) -> Result<(HashMap<String, CommandPos>, HashMap<u64, u64>)> {
        let reader = BufReader::new(File::open(dir.join("keydir.hint"))?);
        let keydir = serde_json::from_reader(reader)?;
        let reader = BufReader::new(File::open(dir.join("dead_bytes.hint"))?);
        let dead_bytes = serde_json::from_reader(reader)?;
        Ok((keydir, dead_bytes))
    }

    fn replay_logs(
        readers: &mut BTreeMap<u64, BufReader<File>>,
    ) -> Result<(HashMap<String, CommandPos>, HashMap<u64, u64>)> {
        let mut keydir = HashMap::new();
        let mut dead_bytes = HashMap::new();
        for (&gen, reader) in readers.iter_mut() {
            let mut pos = 0;
            let mut stream = Deserializer::from_reader(reader).into_iter();
            while let Some(cmd) = stream.next() {
                let new_pos = stream.byte_offset() as u64;
                let len = new_pos - pos;
                match cmd? {
                    Command::Set { key, .. } => {
                        let cmd_pos = CommandPos { gen, pos, len };
                        if let Some(old) = keydir.insert(key, cmd_pos) {
                            *dead_bytes.entry(old.gen).or_insert(0) += old.len;
                        }
                    }
                    Command::Remove { key, .. } => match keydir.remove(&key) {
                        Some(old) => *dead_bytes.entry(old.gen).or_insert(0) += old.len,
                        None => *dead_bytes.entry(gen).or_insert(0) += len,
                    },
                }
                pos = new_pos;
            }
        }
        Ok((keydir, dead_bytes))
    }

    fn use_next_gen(&mut self) -> Result<()> {
        self.active_gen += 1;
        let path = get_log_path(&self.dir, self.active_gen);
        self.writer = BufWriter::new(
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)?,
        );
        self.writer.seek(SeekFrom::End(0))?;
        self.readers
            .insert(self.active_gen, BufReader::new(File::open(path)?));
        Ok(())
    }

    fn write_command(&mut self, cmd: Command) -> Result<(u64, u64)> {
        let len = self.writer.get_ref().metadata()?.len();
        if len >= MAX_FILE_SIZE {
            self.use_next_gen()?;
            self.compact(self.active_gen - 1)?;
        }
        let pos = self.writer.stream_position()?;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        let len = self.writer.stream_position()? - pos;
        self.writer.flush()?;
        Ok((pos, len))
    }

    fn compact(&mut self, gen: u64) -> Result<()> {
        let len = *self.dead_bytes.get(&gen).unwrap_or(&0);
        if len <= COMPACTION_THRESHOLD || gen == self.active_gen {
            return Ok(());
        }
        self.dead_bytes.remove(&gen);

        let mut reader = self.readers.remove(&gen).unwrap();
        reader.seek(SeekFrom::Start(0))?;
        let mut stream = Deserializer::from_reader(reader).into_iter();
        let mut file_size = self.writer.get_ref().metadata()?.len();
        while let Some(cmd) = stream.next() {
            let cmd = cmd?;
            let end = stream.byte_offset() as u64;
            match &cmd {
                Command::Set { key, .. } => match self.keydir.get_mut(key) {
                    Some(cmd_pos) => {
                        if cmd_pos.gen != gen || cmd_pos.pos + cmd_pos.len != end {
                            continue;
                        }
                        cmd_pos.gen = self.active_gen;
                        cmd_pos.pos = self.writer.stream_position()?
                    }
                    None => continue,
                },
                Command::Remove { key, gen } => {
                    if self.keydir.get(key).is_some()
                        || *gen == self.active_gen
                        || !get_log_path(&self.dir, *gen).exists()
                    {
                        continue;
                    }
                }
            }
            serde_json::to_writer(&mut self.writer, &cmd)?;
            file_size += stream.byte_offset() as u64 - end;
            if file_size >= MAX_FILE_SIZE {
                self.use_next_gen()?;
                file_size = 0;
            }
        }
        self.writer.flush()?;
        fs::remove_file(get_log_path(&self.dir, gen))?;

        Ok(())
    }
}

impl KvsEngine for KvStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let (pos, len) = self.write_command(Command::Set {
            key: key.clone(),
            value,
        })?;
        let cmd_pos = CommandPos {
            gen: self.active_gen,
            pos,
            len,
        };
        if let Some(old) = self.keydir.insert(key, cmd_pos) {
            *self.dead_bytes.entry(old.gen).or_insert(0) += old.len;
            self.compact(old.gen)?;
        }

        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.keydir.get(&key) {
            Some(&CommandPos { gen, pos, len }) => {
                let reader = self.readers.get_mut(&gen).unwrap();
                reader.seek(SeekFrom::Start(pos))?;
                let reader = reader.take(len);
                match serde_json::from_reader(reader)? {
                    Command::Set { value, .. } => Ok(Some(value)),
                    rm => panic!("Wrong remove command: {:?}", rm),
                }
            }
            None => Ok(None),
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        match self.keydir.remove(&key) {
            Some(old) => {
                let (_, len) = self.write_command(Command::Remove { key, gen: old.gen })?;
                *self.dead_bytes.entry(old.gen).or_insert(0) += old.len;
                if old.gen == self.active_gen {
                    *self.dead_bytes.entry(self.active_gen).or_insert(0) += len;
                }
                self.compact(old.gen)?;
                Ok(())
            }
            None => Err(KvsError::KeyNotFound),
        }
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        let writer = BufWriter::new(File::create(self.dir.join("keydir.hint")).unwrap());
        serde_json::to_writer(writer, &self.keydir).unwrap();
        let writer = BufWriter::new(File::create(self.dir.join("dead_bytes.hint")).unwrap());
        serde_json::to_writer(writer, &self.dead_bytes).unwrap();
    }
}
