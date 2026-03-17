# Velr

Velr is an embedded property-graph database from Velr.ai, written in Rust, built on top of SQLite, and queried using **openCypher**.

It runs in-process, persists to a standard SQLite database file, and is designed for local, embedded, and edge use cases.

This repository is the **public entry point** for Velr: the best place to discover the project, find the main public resources, ask questions, report bugs, and explore how to get started.

We’d love to have you join the Velr community.

## Release status

Velr is currently in **public alpha** and is released under a **Free Binary Redistribution License**.

- The API and query support are still evolving.
- openCypher coverage is already substantial, but some features are still missing.

Velr is already usable for real workflows and representative use cases, but rough edges remain and the API is not yet stable.

## Roadmap direction

Velr 1.0 is focused on delivering strong **openCypher compatibility** for an embedded, SQLite-based graph database.

After 1.0, planned capabilities include:

- vector search
- time-series
- federation

## What you can do here

This repository is the **public hub** for Velr and the best place to get started.

Here you can:

- **join community discussions and ask questions** in [GitHub Discussions](https://github.com/velr-ai/velr/discussions)
- **report bugs and request features** in [GitHub Issues](https://github.com/velr-ai/velr/issues)
- **find the main public Velr resources**, including packages and documentation

We’d love to hear what you’re building with Velr, answer questions, and get feedback as the project evolves.

## Getting started

Velr is available today as a **Rust crate** and a **Python package**.

### Rust

- [velr on crates.io](https://crates.io/crates/velr)
- [velr API docs on docs.rs](https://docs.rs/velr/latest/velr/)
- [velr-rust-driver](https://github.com/velr-ai/velr-rust-driver)

### Python

- [velr on PyPI](https://pypi.org/project/velr/)
- [velr-python-examples](https://github.com/velr-ai/velr-python-examples)

### Website

- [velr.ai](https://velr.ai/)

## Example repositories

If you want the fastest path to working code, start here:

- [velr-rust-examples](https://github.com/velr-ai/velr-rust-examples) — end-to-end Rust examples and common workflows
- [velr-python-examples](https://github.com/velr-ai/velr-python-examples) — end-to-end Python examples and common workflows

## Why Velr

Velr is designed for applications that need graph queries without running a separate database server.

It is built for:

- embedded applications
- local-first software
- edge systems
- agent memory
- modern Rust and Python workflows

## License

Velr is currently distributed under a **Free Binary Redistribution License**.