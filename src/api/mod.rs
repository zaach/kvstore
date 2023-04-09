use crate::core::storage::KeyValueStorage;
use bytes::Bytes;
use rouille::{router, Request, Response};

pub fn run(port: u16, storage: impl KeyValueStorage + Sync) -> Result<(), Box<dyn std::error::Error>> {
    rouille::start_server(format!("0.0.0.0:{:?}", port), move |request| {
        let mut local = storage.clone();
        router!(request,
            (GET) (/{key: String}) => {
                let value = local.get(Bytes::from(key)).unwrap();
                Response::text(std::str::from_utf8(&value.unwrap()).unwrap())
            },
            _ => Response::empty_404()
        )
    });
}
