mod core;
mod util;

use crate::core::storage::{KVContext, KeyValueStorage};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut kv = KVContext::from_dir(Some(".kvstore/data")).unwrap();
    kv.set("key".into(), "value".into()).unwrap();
    let val = kv.get("key".into()).unwrap();
    println!("val: {:?}", val);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn it_gets_none_for_empty() {
        let mut kv = KVContext::from_dir(Some(".test/data")).unwrap();

        assert_eq!(kv.get("empty".into()).unwrap(), None);
    }

    #[test]
    fn it_sets_and_gets() {
        let mut kv = KVContext::from_dir(Some(".test/data")).unwrap();

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
        let mut kv = KVContext::from_dir(Some(".test/data")).unwrap();

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
