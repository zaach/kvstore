mod rouille;

use crate::core::KeyValueStorage;
use anyhow::Result;

pub trait ApiServer {
    type Storage: KeyValueStorage + Sync;

    fn run(&self, port: u16) -> Result<()>;
}

pub use self::rouille::RouilleApiServer;
