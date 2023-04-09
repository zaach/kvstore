mod cli;
mod core;
mod util;
mod api;

use crate::core::storage::KVContext;
use clap::Parser;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut cli = crate::cli::Cli::parse();

    let data_dir = cli.data_dir.take();

    let storage = KVContext::from_dir(data_dir)?;

    crate::cli::run(cli, storage)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    //use super::*;
    use crate::core::storage::{KVContext, KeyValueStorage};
    use bytes::Bytes;
    use std::path::PathBuf;

    #[test]
    fn it_gets_none_for_empty() {
        let mut kv = KVContext::from_dir(Some(PathBuf::from(".test/data"))).unwrap();

        assert_eq!(kv.get("empty".into()).unwrap(), None);
    }

    #[test]
    fn it_sets_and_gets() {
        let mut kv = KVContext::from_dir(Some(PathBuf::from(".test/data"))).unwrap();

        let value = Bytes::from("value");
        kv.set("key".into(), value).unwrap();
        assert_eq!(kv.get("key".into()).unwrap().unwrap(), Bytes::from("value"));

        kv.set("key".into(), Bytes::from("updated")).unwrap();
        assert_eq!(
            kv.get("key".into()).unwrap().unwrap(),
            Bytes::from("updated")
        );
    }

    #[test]
    fn it_del() {
        let mut kv = KVContext::from_dir(Some(PathBuf::from(".test/data"))).unwrap();

        kv.set("new key".into(), Bytes::from("new val")).unwrap();
        assert_eq!(
            kv.get("new key".into()).unwrap().unwrap(),
            Bytes::from("new val")
        );
        kv.del("new key".into()).unwrap();
        assert_eq!(kv.get("new key".into()).unwrap(), None);

        // remove the test directory
        let test_dir = std::env::current_dir().unwrap().join(".test/data");
        std::fs::remove_dir_all(test_dir).unwrap();
    }
}
