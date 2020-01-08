use kvs::{KvsClient, Result};

use std::net::SocketAddr;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
struct Opt {
    #[structopt(short, long, default_value = "127.0.0.1:4000")]
    addr: SocketAddr,

    #[structopt(subcommand)]
    cmd: Command,
}

#[derive(StructOpt, Debug)]
pub enum Command {
    /// Set the value of a key
    Set { key: String, value: String },

    /// Get the value of a key
    Get { key: String },

    /// Delete a key
    Rm { key: String },
}

fn main() {
    let opt = Opt::from_args();
    if let Err(e) = run(opt) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

fn run(opt: Opt) -> Result<()> {
    let mut client = KvsClient::new(opt.addr)?;
    match opt.cmd {
        Command::Get { key } => client.get(key).map(|value| match value {
            Some(value) => println!("{}", value),
            None => println!("Key not found"),
        }),
        Command::Set { key, value } => client.set(key, value),
        Command::Rm { key } => client.remove(key),
    }
}
