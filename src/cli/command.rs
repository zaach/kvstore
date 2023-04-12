mod runner;

use clap::Subcommand;

#[derive(Subcommand)]
pub enum Commands {
    // Set a key-value pair
    Set {
        key: Option<String>,
        value: Option<String>,
    },
    Get {
        key: Option<String>,
    },
    Del {
        key: Option<String>,
    },
    // Start server
    Server {
        #[arg(short, long, default_value = "5555")]
        port: Option<u16>,
    },
}

pub use self::runner::CommandRunner;
