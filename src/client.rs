use async_std::net::{TcpStream, ToSocketAddrs};

use super::{receive, send, KvsError, Request, Result};

type Response = std::result::Result<Option<String>, String>;

pub struct KvsClient {
    stream: TcpStream,
}

impl KvsClient {
    pub async fn new(addr: impl ToSocketAddrs) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(KvsClient { stream })
    }

    pub async fn set(&mut self, key: String, value: String) -> Result<()> {
        send(&mut self.stream, &Request::Set { key, value }).await?;
        let resp: Response = bincode::deserialize(&receive(&mut self.stream).await?)?;
        resp.map(|_| ()).map_err(KvsError::Server)
    }

    pub async fn get(&mut self, key: String) -> Result<Option<String>> {
        send(&mut self.stream, &Request::Get { key }).await?;
        let resp: Response = bincode::deserialize(&receive(&mut self.stream).await?)?;
        resp.map_err(KvsError::Server)
    }

    pub async fn remove(&mut self, key: String) -> Result<()> {
        send(&mut self.stream, &Request::Remove { key }).await?;
        let resp: Response = bincode::deserialize(&receive(&mut self.stream).await?)?;
        resp.map(|_| ()).map_err(KvsError::Server)
    }
}
