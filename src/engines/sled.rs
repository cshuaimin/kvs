use crate::{KvsEngine, KvsError, Result};
use sled;

pub struct Sled(sled::Db);

impl Sled {
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        Ok(Sled(sled::open(path)?))
    }
}

impl KvsEngine for Sled {
    fn set(&self, key: String, value: String) -> Result<()> {
        Ok(self
            .0
            .insert(key, value.into_bytes())
            // .and_then(|_| self.0.flush())
            .map(|_| ())?)
    }
    fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self
            .0
            .get(key)?
            .map(|v| v.to_vec())
            .map(String::from_utf8)
            .transpose()?)
    }
    fn remove(&self, key: String) -> Result<()> {
        match self.0.remove(key) {
            Ok(Some(_)) => Ok(self.0.flush().and_then(|_| Ok(()))?),
            Ok(None) => Err(KvsError::KeyNotFound),
            Err(e) => Err(e.into()),
        }
    }
}