use crate::api::ApiServer;
use crate::core::KeyValueStorage;
use anyhow::Result;
use std::io::{self, Write};
use thiserror::Error;
use super::Commands;

#[derive(Error, Debug)]
pub enum CliError {
    #[error("Missing key")]
    MissingKey,
    #[error("Missing value")]
    MissingValue,
    #[error("Invalid port")]
    InvalidPort,
}

pub struct CommandRunner<S: KeyValueStorage, T: ApiServer> {
    storage: S,
    api_server: T,
}

impl<S: KeyValueStorage, T: ApiServer> CommandRunner<S, T> {
    pub fn new(storage: S, api_server: T) -> Self {
        Self {
            storage,
            api_server,
        }
    }

    pub fn run(&mut self, command: &Option<Commands>) -> Result<()> {
        match command {
            Some(Commands::Set { key, value }) => {
                set_key(&mut self.storage, key, value)?;
            }
            Some(Commands::Get { key }) => {
                get_key(&mut self.storage, key)?;
            }
            Some(Commands::Del { key }) => {
                del_key(&mut self.storage, key)?;
            }
            Some(Commands::Server { port }) => {
                run_server(&self.api_server, port)?;
            }
            None => {}
        }

        Ok(())
    }
}

fn set_key(
    storage: &mut impl KeyValueStorage,
    key: &Option<String>,
    value: &Option<String>,
) -> Result<()> {
    match (key, value) {
        (Some(key), Some(value)) => {
            storage.set(key.to_string().into(), value.to_string().into())?;
        }
        (_, None) => {
            return Err(CliError::MissingValue.into());
        }
        (None, _) => {
            return Err(CliError::MissingKey.into());
        }
    }
    Ok(())
}

fn get_key(storage: &mut impl KeyValueStorage, key: &Option<String>) -> Result<()> {
    match key {
        Some(k) => {
            let val = storage.get(k.to_string().into())?;
            if let Some(v) = val {
                io::stdout().write_all(&v)?;
            }
        }
        None => {
            return Err(CliError::MissingKey.into());
        }
    }
    Ok(())
}

fn del_key(storage: &mut impl KeyValueStorage, key: &Option<String>) -> Result<()> {
    match key {
        Some(k) => {
            storage.del(k.to_string().into())?;
        }
        None => {
            return Err(CliError::MissingKey.into());
        }
    }
    Ok(())
}

fn run_server(server: &impl ApiServer, port: &Option<u16>) -> Result<()> {
    match port {
        Some(p) => {
            server.run(*p)?;
        }
        None => {
            return Err(CliError::InvalidPort.into());
        }
    }
    Ok(())
}
