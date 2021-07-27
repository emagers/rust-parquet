# rust-parquet
A CLI tool for inspecting Parquet files

# Building locally

1. Make sure you have the [Rust tools installed](https://www.rust-lang.org/tools/install)
1. Run `cargo build` or `cargo build --release`
1. The release executable will be output to `/target/release/`, you can run a debug version by executing `cargo run ...`

# Command Options

1. `rust-parquet <filename>` outputs the file metadata
1. `rust-parquet count <filename>` outputs the row count
1. `rust-parquet schema <filename>` outputs the schema
1. `rust-parquet display -c 5 <filename>` outputs the top 5 rows
1. `rust-parquet display -c 5 -f csv <filename>` outputs the top 5 rows in CSV format