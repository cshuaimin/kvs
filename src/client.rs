use super::{KvsError, Request, Result};
use serde_json::{self, de::IoRead, Deserializer, StreamDeserializer};
use std::{
    io::{prelude::*, BufReader, BufWriter},
    net::{TcpStream, ToSocketAddrs},
};

type Response = std::result::Result<Option<String>, String>;

pub struct KvsClient<'de> {
    responses: StreamDeserializer<'de, IoRead<BufReader<TcpStream>>, Response>,
    writer: BufWriter<TcpStream>,
}

impl<'de> KvsClient<'de> {
    pub fn new(addr: impl ToSocketAddrs) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;
        let writer = BufWriter::new(stream.try_clone()?);
        let responses = Deserializer::from_reader(BufReader::new(stream)).into_iter();
        Ok(KvsClient { responses, writer })
    }

    fn send_request(&mut self, request: Request) -> Result<Option<String>> {
        serde_json::to_writer(&mut self.writer, &request)?;
        self.writer.flush()?;
        match self.responses.next() {
            None => Err(KvsError::StringError(
                "lost connection to server".to_string(),
            )),
            Some(response) => response?.map_err(KvsError::StringError),
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.send_request(Request::Set { key, value }).map(|_| ())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        self.send_request(Request::Get { key })
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        self.send_request(Request::Remove { key }).map(|_| ())
    }
}
