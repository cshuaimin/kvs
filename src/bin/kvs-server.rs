use env_logger;
use kvs::{start_server, Result};
use log::info;
use std::net::SocketAddr;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
struct Opt {
    /// Address to listen
    #[structopt(short, long, default_value = "127.0.0.1:4000")]
    addr: SocketAddr,

    /// Storage engine to use
    #[structopt(short, long, default_value = "kvs")]
    engine: String,
}

fn main() -> Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    let opt = Opt::from_args();
    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {}", opt.engine);
    info!("Listening on {}", opt.addr);

    if let Err(e) = start_server(opt.addr, &opt.engine) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
    Ok(())
}
