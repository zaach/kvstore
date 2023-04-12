pub mod file;

use bytes::Bytes;

pub trait KeyValueStorage: Clone + Send + 'static {
    type Error: std::error::Error + Send + Sync;

    fn set(&mut self, key: Bytes, value: Bytes) -> Result<(), Self::Error>;

    fn get(&mut self, key: Bytes) -> Result<Option<Bytes>, Self::Error>;

    fn del(&mut self, key: Bytes) -> Result<bool, Self::Error>;
}

