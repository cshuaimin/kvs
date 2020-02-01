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
use failure::Fail;
use serde::{Deserialize, Serialize};
use std::string::FromUtf8Error;
use std::{io, num::ParseIntError};

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

async fn receive(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut len = [0u8; 8];
    stream.read_exact(&mut len).await?;
    let len = usize::from_be_bytes(len);
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

#[derive(Fail, Debug)]
pub enum KvsError {
    #[fail(display = "{}", _0)]
    Io(#[fail(cause)] io::Error),

    #[fail(display = "{}", _0)]
    Parse(#[fail(cause)] ParseIntError),

    // #[fail(display = "{}", _0)]
    // Serde(#[fail(cause)] serde_json::Error),

    // #[fail(display = "sled error: {}", _0)]
    // Sled(#[fail(cause)] sled::Error),
    #[fail(display = "UTF-8 error: {}", _0)]
    Utf8(#[fail(cause)] FromUtf8Error),

    #[fail(display = "Key not found")]
    KeyNotFound,

    #[fail(display = "{}", _0)]
    StringError(String),
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::Io(err)
    }
}

impl From<ParseIntError> for KvsError {
    fn from(err: ParseIntError) -> KvsError {
        KvsError::Parse(err)
    }
}

// impl From<serde_json::Error> for KvsError {
//     fn from(err: serde_json::Error) -> KvsError {
//         KvsError::Serde(err)
//     }
// }

// impl From<sled::Error> for KvsError {
//     fn from(err: sled::Error) -> KvsError {
//         KvsError::Sled(err)
//     }
// }

impl From<FromUtf8Error> for KvsError {
    fn from(err: FromUtf8Error) -> KvsError {
        KvsError::Utf8(err)
    }
}

impl From<&str> for KvsError {
    fn from(err: &str) -> KvsError {
        KvsError::StringError(err.to_string())
    }
}

impl From<String> for KvsError {
    fn from(err: String) -> KvsError {
        KvsError::StringError(err)
    }
}

pub type Result<T> = std::result::Result<T, KvsError>;
