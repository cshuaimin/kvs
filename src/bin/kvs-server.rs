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
}

fn main() -> Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    let opt = Opt::from_args();
    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Listening on {}", opt.addr);

    if let Err(e) = async_std::task::block_on(start_server(opt.addr)) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
    Ok(())
}
