use std::env::current_dir;

use async_std::io::ErrorKind;
use async_std::net::{TcpListener, TcpStream, ToSocketAddrs};
use async_std::prelude::*;
use async_std::task;
use log::warn;

use super::{receive, send, KvStore, Request, Result};

pub async fn start_server(addr: impl ToSocketAddrs) -> Result<()> {
    let kvs = KvStore::open(current_dir()?).await?;
    let listener = TcpListener::bind(addr).await?;

    let mut incoming = listener.incoming();
    while let Some(stream) = incoming.next().await {
        let mut stream = stream?;
        let kvs = kvs.clone();
        task::spawn(async move {
            if let Err(e) = serve(&mut stream, kvs).await {
                warn!("Error serving {}: {}", stream.peer_addr().unwrap(), e);
            }
        });
    }
    Ok(())
}

async fn serve(stream: &mut TcpStream, kvs: KvStore) -> Result<()> {
    loop {
        let response = match receive(stream).await {
            Ok(buf) => match bincode::deserialize(&buf).unwrap() {
                Request::Get { key } => kvs.get(key).await,
                Request::Set { key, value } => kvs.set(key, value).await.map(|()| None),
                Request::Remove { key } => kvs.remove(key).await.map(|()| None),
            },
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e.into()),
        }
        .map_err(|e| e.to_string());
        send(stream, &response).await?;
    }
}
