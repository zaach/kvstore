# Yet another toy key-value store written in rust

This is a learning project. The goal was to implement something interesting in rust using a Clean/Hexagonal Architecture. A thread-safe Bitcask-like key-value store with a CLI and REST API fit the bill nicely. Inspired by these articles:

- [Build a BLAZINGLY FAST key-value store with Rust](https://www.tunglevo.com/note/build-a-blazingly-fast-key-value-store-with-rust/)
- [Data-oriented, clean&hexagonal architecture software in Rust – through an example project](https://dpc.pw/data-oriented-cleanandhexagonal-architecture-software-in-rust-through-an-example)
- [Hexagonal architecture in Rust ](https://alexis-lozano.com/hexagonal-architecture-in-rust-1/)

## Build

```
cargo build
```

## Usage
```
cargo run -- help
Usage: kvstore [OPTIONS] [COMMAND]

Commands:
  set
  get
  del
  server
  help    Print this message or the help of the given subcommand(s)

Options:
  -d, --data-dir <DATA_DIR>
  -h, --help                 Print help
  -V, --version              Print version
```
