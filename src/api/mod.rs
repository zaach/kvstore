mod app;

use crate::core::storage::KeyValueStorage;
use rouille::{router, Response};

pub fn run(
    port: u16,
    storage: impl KeyValueStorage + Sync,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting server on port {}", port);
    rouille::start_server(format!("0.0.0.0:{:?}", port), move |request| {
        let mut local = storage.clone();
        rouille::log(&request, std::io::stdout(), || {
            router!(request,
                (GET) (/{key: String}) => {
                    app::get_key(&mut local, request, key)
                },
                (POST) (/{key: String}) => {
                    app::set_key(&mut local, request, key)
                },
                (DELETE) (/{key: String}) => {
                    app::del_key(&mut local, request, key)
                },
                _ => Response::empty_404()
            )
        })
    });
}
