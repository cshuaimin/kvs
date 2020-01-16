use std::{
    env::current_dir,
    fs,
    io::{prelude::*, BufReader, BufWriter},
    net::{TcpListener, TcpStream, ToSocketAddrs},
    thread,
    time::Duration,
};

use crossbeam_channel::{select, Sender};
use log::{debug, error};
use serde_json::{self, Deserializer};

use super::{KvStore, KvsEngine, Request, Result, Sled};

pub fn start_server(addr: impl ToSocketAddrs, engine: &str) -> Result<()> {
    let path = current_dir()?.join("engine");
    if let Ok(current) = fs::read_to_string(&path) {
        if current != engine {
            return Err(format!("The engine {} does not match current {}", engine, current).into());
        }
    }
    let save_engine = || fs::write(path, engine);
    match engine {
        "kvs" => {
            save_engine()?;
            let engine = KvStore::open(current_dir()?)?;
            serve_with_engine(addr, engine)
        }
        "sled" => {
            save_engine()?;
            let engine = Sled::open(current_dir()?)?;
            serve_with_engine(addr, engine)
        }
        other => Err(format!("Unknown engine {}", other).into()),
    }
}

fn serve_with_engine(addr: impl ToSocketAddrs, engine: impl KvsEngine) -> Result<()> {
    let listener = TcpListener::bind(addr)?;
    let tx = start_threads(engine);
    for stream in listener.incoming() {
        tx.send(stream?).unwrap();
    }
    Ok(())
}

fn start_threads(engine: impl KvsEngine) -> Sender<TcpStream> {
    let (req_tx, req_rx) = crossbeam_channel::unbounded();
    let mut close_readers = Vec::with_capacity(4);

    for _ in 0..4 {
        let (close_reader_tx, close_reader_rx) = crossbeam_channel::bounded(0);
        close_readers.push(close_reader_tx);
        let req_rx = req_rx.clone();
        let engine = engine.clone();
        thread::spawn(move || loop {
            select! {
                recv(req_rx) -> stream => {
                    let stream = stream.unwrap();
                    if let Err(e) = handle_requests(&stream, &engine) {
                        let addr = stream.peer_addr().unwrap();
                        error!("Error handling requests from {}", addr);
                    }
                }
                recv(close_reader_rx) -> gen => {
                    let gen = gen.unwrap();
                    engine.close_reader(gen);
                }
            }
        });
    }
    thread::spawn(move || loop {
        thread::sleep(Duration::from_secs(100));
        let gen = engine.compact().unwrap();
        for tx in close_readers {
            tx.send(gen).unwrap();
        }
    });
    req_tx
}

fn handle_requests(stream: &TcpStream, engine: &impl KvsEngine) -> Result<()> {
    let reader = BufReader::new(stream);
    let mut writer = BufWriter::new(stream);
    let requests = Deserializer::from_reader(reader).into_iter::<Request>();

    for request in requests {
        let request = request?;
        debug!(
            "Received request from {}: {:?}",
            stream.peer_addr()?,
            request
        );
        let response = match request {
            Request::Get { key } => engine.get(key).map_err(|e| e.to_string()),
            Request::Set { key, value } => engine
                .set(key, value)
                .map(|()| None)
                .map_err(|e| e.to_string()),
            Request::Remove { key } => engine.remove(key).map(|()| None).map_err(|e| e.to_string()),
        };
        debug!(
            "Sending response to {}: {:?}",
            stream.peer_addr()?,
            response
        );
        serde_json::to_writer(&mut writer, &response)?;
        writer.flush()?;
    }
    Ok(())
}
