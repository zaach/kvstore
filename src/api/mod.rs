use crate::core::storage::KeyValueStorage;
use bytes::Bytes;
use rouille::{input::post::BufferedFile, post_input, router, try_or_400, Response};
use std::io;

pub fn run(
    port: u16,
    storage: impl KeyValueStorage + Sync,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting server on port {}", port);
    rouille::start_server(format!("0.0.0.0:{:?}", port), move |request| {
        let mut local = storage.clone();
        rouille::log(&request, io::stdout(), || {
            router!(request,
                (GET) (/{key: String}) => {
                    let result = local.get(Bytes::from(key));
                    match result {
                        Ok(Some(value)) => Response::from_data("application/octet-stream", value),
                        Ok(None) => Response::empty_404(),
                        Err(e) => Response::text(format!("Error: {}", e)).with_status_code(500),
                    }
                },
                (POST) (/{key: String}) => {
                    let input = try_or_400!(post_input!(request, {
                        value: Option<String>,
                        file: Option<BufferedFile>,
                    }));

                    match (input.value, input.file) {
                        (Some(value), None) => {
                            let result = local.set(Bytes::from(key), Bytes::from(value));
                            match result {
                                Ok(_) => Response::empty_204(),
                                Err(e) => Response::text(format!("Error: {}", e)).with_status_code(500),
                            }
                        },
                        (None, Some(file)) => {
                            let result = local.set(Bytes::from(key), Bytes::from(file.data));
                            match result {
                                Ok(_) => Response::empty_204(),
                                Err(e) => Response::text(format!("Error: {}", e)).with_status_code(500),
                            }
                        },
                        _ => Response::empty_400(),
                    }
                },
                (DELETE) (/{key: String}) => {
                    let result = local.del(Bytes::from(key));
                    match result {
                        Ok(true) => Response::empty_204(),
                        Ok(false) => Response::empty_404(),
                        Err(e) => Response::text(format!("Error: {}", e)).with_status_code(500),
                    }
                },
                _ => Response::empty_404()
            )
        })
    });
}
