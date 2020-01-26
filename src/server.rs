// use async_std::fs;
// use async_std::sync::Arc;
// use async_std::task;
// use async_std::net::{TcpListener, TcpStream, ToSocketAddrs};
// use async_std::prelude::*;
// use std::env::current_dir;
// use std::time::Duration;

// use log::{debug, error};

// use crate::{KvStore, Request, Result};

// pub async fn start_server(addr: impl ToSocketAddrs) -> Result<()> {
//     let kvs = Arc::new(KvStore::open(current_dir()?).await?);
//     let listener = TcpListener::bind(addr).await?;

//     let mut incoming = listener.incoming();
//     while let Some(stream) = incoming.next().await {
//         task::spawn(serve(stream?, Arc::clone(&kvs)));
//     }
//     Ok(())
// }

// async fn serve(stream: TcpStream, kvs: Arc<KvStore>) -> Result<()> {
//     kvs.get("hi").await.unwrap();
//     Ok(())
// }