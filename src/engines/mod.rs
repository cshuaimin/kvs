mod kvs;
mod sled;

pub use self::kvs::KvStore;
pub use self::sled::Sled;

use crate::Result;

pub trait KvsEngine: Clone {
    fn set(&self, key: String, value: String) -> Result<()>;
    fn get(&self, key: String) -> Result<Option<String>>;
    fn remove(&self, key: String) -> Result<()>;
    fn close_reader(&self, gen: u64);
    fn compact(&self) -> Result<u64>;
}
