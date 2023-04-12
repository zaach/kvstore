mod command;

use clap::Parser;
use std::path::PathBuf;
use self::command::Commands;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[arg(short, long)]
    pub data_dir: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

pub use self::command::CommandRunner;
