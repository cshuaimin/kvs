#![feature(seek_convenience)]

mod client;
mod engines;
mod server;
pub mod thread_pool;

pub use client::KvsClient;
pub use engines::{KvStore, KvsEngine, Sled};
pub use server::start_server;

use failure::Fail;
use serde::{Deserialize, Serialize};
use std::string::FromUtf8Error;
use std::{io, num::ParseIntError};

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Set { key: String, value: String },
    Get { key: String },
    Remove { key: String },
}

#[derive(Fail, Debug)]
pub enum KvsError {
    #[fail(display = "{}", _0)]
    Io(#[fail(cause)] io::Error),

    #[fail(display = "{}", _0)]
    Parse(#[fail(cause)] ParseIntError),

    #[fail(display = "{}", _0)]
    Serde(#[fail(cause)] serde_json::Error),

    #[fail(display = "sled error: {}", _0)]
    Sled(#[fail(cause)] sled::Error),

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

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> KvsError {
        KvsError::Serde(err)
    }
}

impl From<sled::Error> for KvsError {
    fn from(err: sled::Error) -> KvsError {
        KvsError::Sled(err)
    }
}

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
