use crate::core::storage::KeyValueStorage;
use bytes::Bytes;
use rouille::{input::post::BufferedFile, post_input, try_or_400, Response};

pub fn get_key(
    storage: &mut impl KeyValueStorage,
    _request: &rouille::Request,
    key: String,
) -> Response {
    let result = storage.get(Bytes::from(key));
    match result {
        Ok(Some(value)) => Response::from_data("application/octet-stream", value),
        Ok(None) => Response::empty_404(),
        Err(e) => Response::text(format!("Error: {}", e)).with_status_code(500),
    }
}

pub fn set_key(
    storage: &mut impl KeyValueStorage,
    request: &rouille::Request,
    key: String,
) -> Response {
    let input = try_or_400!(post_input!(request, {
        value: Option<String>,
        file: Option<BufferedFile>,
    }));

    match (input.value, input.file) {
        (Some(value), None) => {
            let result = storage.set(Bytes::from(key), Bytes::from(value));
            match result {
                Ok(_) => Response::empty_204(),
                Err(e) => Response::text(format!("Error: {}", e)).with_status_code(500),
            }
        }
        (None, Some(file)) => {
            let result = storage.set(Bytes::from(key), Bytes::from(file.data));
            match result {
                Ok(_) => Response::empty_204(),
                Err(e) => Response::text(format!("Error: {}", e)).with_status_code(500),
            }
        }
        _ => Response::empty_400(),
    }
}

pub fn del_key(
    storage: &mut impl KeyValueStorage,
    _request: &rouille::Request,
    key: String,
) -> Response {
    let result = storage.del(Bytes::from(key));
    match result {
        Ok(true) => Response::empty_204(),
        Ok(false) => Response::empty_404(),
        Err(e) => Response::text(format!("Error: {}", e)).with_status_code(500),
    }
}
