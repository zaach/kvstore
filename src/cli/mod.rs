use clap::{Parser, Subcommand};
use crate::core::storage::KeyValueStorage;
use std::path::PathBuf;
use bytes::Bytes;
use crate::api;
use std::io::{self, Write};


#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[arg(short, long)]
    pub data_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    // Set a key-value pair
    Set {
        key: Option<String>,
        value: Option<String>,
    },
    Get {
        key: Option<String>,
    },
    Del {
        key: Option<String>,
    },
    // Start server
    Server {
        #[arg(short, long, default_value = "5555")]
        port: Option<u16>,
    },
}


pub fn run(cli: Cli, mut storage: impl KeyValueStorage + Sync) -> Result<(), Box<dyn std::error::Error>> {
    match &cli.command {
        Some(Commands::Set { key, value }) => {
            if let Some(k) = key {
                if let Some(v) = value {
                    storage.set(Bytes::from(k.to_string()), Bytes::from(v.to_string()))?;
                }
            }
        }
        Some(Commands::Get { key }) => {
            if let Some(k) = key {
                let val = storage.get(Bytes::from(k.to_string()))?;
                if let Some(v) = val {
                    io::stdout().write_all(&v)?;
                }
            }
        }
        Some(Commands::Del { key }) => {
            if let Some(k) = key {
                storage.del(Bytes::from(k.to_string()))?;
            }
        }
        Some(Commands::Server { port }) => {
            if let Some(p) = port {
                api::run(*p, storage)?;
            }
        }
        None => {}
    }

    Ok(())
}
