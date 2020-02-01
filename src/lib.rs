mod client;
mod kvs;
mod server;
mod skipmap;

pub use self::kvs::KvStore;
pub use client::KvsClient;
pub use server::start_server;
use skipmap::SkipMap;

use async_std::net::TcpStream;
use async_std::prelude::*;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Serialize, Deserialize, Debug)]
enum Request {
    Set { key: String, value: String },
    Get { key: String },
    Remove { key: String },
}

async fn send<T: Serialize>(stream: &mut TcpStream, data: &T) -> Result<()> {
    let data = bincode::serialize(data).unwrap();
    stream.write_all(&data.len().to_be_bytes()).await?;
    stream.write_all(&data).await?;
    Ok(())
}

async fn receive(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut len = [0u8; 8];
    stream.read_exact(&mut len).await?;
    let len = usize::from_be_bytes(len);
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

#[derive(Error, Debug)]
pub enum KvsError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serde error: {0}")]
    Serde(#[from] bincode::Error),

    #[error("key not found")]
    KeyNotFound,

    #[error("server error: {0}")]
    Server(String),
}

pub type Result<T> = std::result::Result<T, KvsError>;
