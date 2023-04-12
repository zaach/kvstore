mod cli;
mod core;
mod util;
mod api;

use crate::core::storage::KvContext;
use clap::Parser;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut cli = crate::cli::Cli::parse();

    let data_dir = cli.data_dir.take();

    let storage = KvContext::from_dir(data_dir)?;

    crate::cli::run(cli, storage)?;

    Ok(())
}

