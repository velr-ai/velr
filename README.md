# Velr

Velr is an embedded property-graph database from Velr.ai, written in Rust, built on top of SQLite, and queried using **openCypher**.

It runs in-process, persists to a standard SQLite database file, and is designed for local, embedded, and edge use cases.

> [!NOTE]
> Velr is currently in **public alpha**.

> [!NOTE]
> **Velr 1.0 is focused on strong openCypher compatibility.**  
> **Vector search**, **time-series**, and **federation** are planned as post-1.0 capabilities.

## What this repository is

This repository is the **public hub** for Velr.

Use it for:

- **community discussions and questions**
- **bug reports and feature requests**
- finding the main public Velr resources

This repository is the public entry point for Velr, with links to the main public resources, packages, examples, and documentation.

## Getting started

Velr is available today as a **Rust crate** and a **Python package**.

### Rust

- [velr on crates.io](https://crates.io/crates/velr)
- [velr API docs on docs.rs](https://docs.rs/velr/latest/velr/)
- [velr-rust-driver](https://github.com/velr-ai/velr-rust-driver)
- [velr-rust-examples](https://github.com/velr-ai/velr-rust-examples)

### Python

- [velr on PyPI](https://pypi.org/project/velr/)
- [velr-python-examples](https://github.com/velr-ai/velr-python-examples)

### Website

- [velr.ai](https://velr.ai/)

## Community

- **Community and questions:** GitHub Discussions
- **Bug reports and feature requests:** GitHub Issues

## Public alpha status

Velr is currently in **public alpha** and is released under a **Free Binary Redistribution License**.

The API and query support are still evolving, but Velr is already usable for many real workflows and application prototypes.

## Roadmap direction

Velr 1.0 is focused on delivering strong **openCypher compatibility** for an embedded, SQLite-based graph database.

After 1.0, planned capabilities include:

- vector search
- time-series
- federation

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