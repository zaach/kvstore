use super::ApiServer;
use crate::core::KeyValueStorage;
use anyhow::Result;
use rouille::{input::post::BufferedFile, post_input, router, try_or_400, Response};

pub struct RouilleApiServer<S: KeyValueStorage + Sync> {
    storage: S,
}

impl<S: KeyValueStorage + Sync> RouilleApiServer<S> {
    pub fn new(storage: S) -> Self {
        Self { storage }
    }
}

impl<S: KeyValueStorage + Sync> ApiServer for RouilleApiServer<S> {
    type Storage = S;

    fn run(&self, port: u16) -> Result<()> {
        let storage = self.storage.clone();
        println!("Starting server on port {}", port);
        rouille::start_server(format!("0.0.0.0:{:?}", port), move |request| {
            let mut local = storage.clone();
            rouille::log(&request, std::io::stdout(), || {
                router!(request,
                    (GET) (/{key: String}) => {
                        get_key(&mut local, request, key)
                    },
                    (POST) (/{key: String}) => {
                        set_key(&mut local, request, key)
                    },
                    (DELETE) (/{key: String}) => {
                        del_key(&mut local, request, key)
                    },
                    _ => Response::empty_404()
                )
            })
        });
    }
}

fn get_key(
    storage: &mut impl KeyValueStorage,
    _request: &rouille::Request,
    key: String,
) -> Response {
    let result = storage.get(key.into());
    match result {
        Ok(Some(value)) => Response::from_data("application/octet-stream", value),
        Ok(None) => Response::empty_404(),
        Err(e) => Response::text(format!("Error: {}", e)).with_status_code(500),
    }
}

fn set_key(
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
            let result = storage.set(key.into(), value.into());
            match result {
                Ok(_) => Response::empty_204(),
                Err(e) => Response::text(format!("Error: {}", e)).with_status_code(500),
            }
        }
        (None, Some(file)) => {
            let result = storage.set(key.into(), file.data.into());
            match result {
                Ok(_) => Response::empty_204(),
                Err(e) => Response::text(format!("Error: {}", e)).with_status_code(500),
            }
        }
        _ => Response::empty_400(),
    }
}

fn del_key(
    storage: &mut impl KeyValueStorage,
    _request: &rouille::Request,
    key: String,
) -> Response {
    let result = storage.del(key.into());
    match result {
        Ok(true) => Response::empty_204(),
        Ok(false) => Response::empty_404(),
        Err(e) => Response::text(format!("Error: {}", e)).with_status_code(500),
    }
}
