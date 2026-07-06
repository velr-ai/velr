Velr CLI
========

`velr` is the command-line shell for Velr. It can run one query and exit, read
queries from stdin, or open an interactive REPL for exploring a Velr database.

Install
-------

Prebuilt archives are published on the
[velr-ai/velr releases page](https://github.com/velr-ai/velr/releases) for
macOS, Linux, and Windows. Download the archive for your platform, unpack it,
and place `velr` or `velr.exe` somewhere on your `PATH`.

```sh
velr --version
velr --help
```

`velr --version` prints both the CLI release and the linked Rust driver version,
for example `velr-cli 0.2.36 (velr driver 0.2.32)`.

Quickstart
----------

Run a query against an in-memory database:

```sh
velr -e 'RETURN 1 AS n'
```

Use a database file:

```sh
velr graph.db -e 'CREATE (:Person {name: "Ada"});'
velr graph.db -e 'MATCH (p:Person) RETURN p.name AS name;'
```

Pipe a script into the CLI:

```sh
printf 'RETURN 1 AS n; RETURN 2 AS m;\n' | velr
```

Explain a query without executing it:

```sh
velr --explain 'MATCH (p:Person) RETURN p.name AS name'
```

Start the interactive shell:

```sh
velr graph.db
```

Execution Modes
---------------

`velr` chooses a mode from the arguments and stdin:

- `velr -e '<query>'` executes query text and exits.
- `velr <db-path> -e '<query>'` executes against a file-backed database.
- `echo '<query>' | velr` reads query text from stdin.
- `velr` starts an in-memory REPL when stdin is a terminal.
- `velr <db-path>` starts a REPL against that database file.
- `velr --explain '<query>'` prints a compact explain trace without executing
  the query.
- `echo '<script>' | velr --explain` explains every statement in the script.

Database paths that start with `-` are rejected so a misspelled flag cannot
silently create a database file. On Windows, `/flag` style arguments are also
rejected for the same reason.

Interactive Shell
-----------------

In the REPL, query input is submitted when the buffer ends with `;`. Pressing
`Enter` before the final semicolon continues the query on the next line.

```text
~/project〉MATCH (p:Person)
:: WHERE p.name = "Ada"
:: RETURN p;
```

REPL commands:

- `:source <file>` executes queries from a file.
- `:pwd` prints the current working directory.
- `:cd [dir]` changes directory; without an argument it goes to your home
  directory.
- `:ls [path]` lists files in the current directory or a provided path.
- `:help` or `:h` shows REPL help.
- `:quit`, `:exit`, `:q`, `Ctrl+C`, or `Ctrl+D` exits.

Example session:

```text
~/project〉:pwd
/Users/sam/project

~/project〉:ls
demo.cypher
data/

~/project〉:source demo.cypher
```

Output
------

Use `--format` or `-f` to choose the output format:

- `styled` uses a terminal-friendly table and is the default on a TTY.
- `plain` uses a simple ASCII table.
- `tsv` is tab-separated and is the default when stdout is not a TTY.
- `csv` is comma-separated.
- `ndjson` emits one JSON object per row.

Examples:

```sh
velr -f plain -e 'MATCH (n) RETURN n'
velr -f csv -e 'MATCH (p:Person) RETURN p.name AS name'
velr -f ndjson -e 'MATCH (p:Person) RETURN p'
```

`--stats` prints timing information to stderr, so stdout remains usable by
scripts:

```sh
velr -f ndjson --stats -e 'MATCH (p:Person) RETURN p.name AS name'
```

Explain
-------

There are two explain workflows:

- `--explain '<query>'` asks the public Rust driver for a compact explain trace
  and does not execute the query.
- `-e 'EXPLAIN ...'` or `-e 'EXPLAIN ANALYZE ...'` executes the statement
  through the driver and renders the result tables in the selected format.

```sh
velr --explain 'RETURN 1 AS n'
velr -f plain -e 'EXPLAIN RETURN 1 AS n'
velr -f ndjson -e 'EXPLAIN ANALYZE RETURN 1 AS n'
```

Scripts
-------

A script may contain multiple semicolon-terminated statements:

```cypher
CREATE (:Person {name: "Ada"});
MATCH (p:Person) RETURN p.name AS name;
```

Run it from the shell:

```sh
velr graph.db < demo.cypher
```

Or from inside the REPL:

```text
~/project〉:source demo.cypher
```

Build From Source
-----------------

End users should normally use the prebuilt release archives. Building from
source is supported for development and verification:

```sh
git clone https://github.com/velr-ai/velr.git
cd velr/rust/velr-cli
cargo build --release
cargo run --release -- --version
```

The CLI source is mirrored under `rust/velr-cli` in `velr-ai/velr`. Release
tags pin the CLI to the selected public Rust driver crate version for that
release. Building from `main` uses the manifest on `main`, which tracks the
latest compatible public Rust driver in the `0.2.x` line.

Exit Codes
----------

- `0`: success
- `1`: query or runtime error
- `2`: usage error

License
-------

The CLI source is licensed under MIT. Prebuilt binary archives also include the
Velr runtime binary redistribution license.
