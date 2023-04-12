mod cli;
mod core;
mod util;
mod api;

use crate::core::KvContext;
use crate::cli::{Cli, CommandRunner};
use crate::api::RouilleApiServer;
use clap::Parser;
use anyhow::Result;

fn main() -> Result<()> {
    let mut cli = Cli::parse();

    let data_dir = cli.data_dir.take();

    let storage = KvContext::from_dir(data_dir)?;
    let api_server = RouilleApiServer::new(storage.clone());
    let mut command_runner = CommandRunner::new(storage, api_server);

    command_runner.run(&cli.command)?;

    Ok(())
}

