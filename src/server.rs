use super::{KvStore, KvsEngine, Request, Result, Sled};
use log::{debug, error};
use serde_json::{self, Deserializer};
use std::{
    env::current_dir,
    fs,
    io::{prelude::*, BufReader, BufWriter},
    net::{TcpListener, TcpStream, ToSocketAddrs},
};

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

fn serve_with_engine(addr: impl ToSocketAddrs, mut engine: impl KvsEngine) -> Result<()> {
    let listener = TcpListener::bind(addr)?;

    for stream in listener.incoming() {
        let stream = stream?;
        if let Err(e) = serve(&stream, &mut engine) {
            error!("Error serving {}: {}", stream.peer_addr()?, e);
        }
    }

    Ok(())
}

fn serve(stream: &TcpStream, engine: &mut impl KvsEngine) -> Result<()> {
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
