Velr CLI
========

The package ships two executables: `velr`, the query shell, and `velr-admin`,
the offline CSV importer. The shell can run one query and exit, read queries
from stdin, or open an interactive REPL for exploring a Velr database.

Use `velr-admin` to create a database from local CSV files, then query and
update it with `velr`.

Install
-------

On macOS, the recommended install path is the Velr Homebrew tap. The same tap
also works on Linux and Windows Subsystem for Linux (WSL) for users who manage
CLI tools with Homebrew:

```sh
brew tap velr-ai/velr
brew install velr
```

If Homebrew is configured to require trust for third-party taps and refuses to
load the formula, trust the Velr formula once and rerun the install:

```sh
brew trust --formula velr-ai/velr/velr
brew install velr
```

The Homebrew formula installs the prebuilt Velr CLI archive for your platform.
It does not require a local Rust toolchain.

For Linux users who prefer direct binaries, Windows users, and manual installs,
prebuilt archives are published on the
[velr-ai/velr releases page](https://github.com/velr-ai/velr/releases) for
macOS, Linux, and Windows. Download the archive for your platform, unpack it,
and place both `velr` and `velr-admin` (with `.exe` on Windows) on your `PATH`.

On macOS, choose the Apple Silicon (`aarch64`) or Intel (`x86_64`) archive
matching your machine. Homebrew selects the matching archive automatically.

The macOS release archive binaries are unsigned. We recommend Homebrew on
macOS. If you still want to use the release archive, remove the quarantine
attribute after unpacking:

```sh
xattr -d com.apple.quarantine /path/to/velr
xattr -d com.apple.quarantine /path/to/velr-admin
```

Verify the install:

```sh
velr --version
velr --help
velr-admin --version
velr-admin --help
```

`velr --version` prints both the CLI release and the Rust driver version.

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

Offline CSV Import
------------------

`velr-admin import` creates a fresh database from Neo4j-compatible CSV input.
The destination argument is a Velr database file. For files with embedded
headers, supply the paths directly:

```sh
velr-admin import graph.db people.csv.gz knows.csv.gz
```

For uncompressed files, use `people.csv` and `knows.csv` as the paths.
Their contents can be:

```text
# people.csv
personId:ID(Person){id-type:long}|name:string|active:boolean|tags:string[]|:LABEL
1|Aster|true|blue;green|Person;Member
2|Birch|false|red|Person;Reader

# knows.csv
:START_ID(Person)|:END_ID(Person)|since:long|:TYPE
1|2|2020|KNOWS
```

The `#` lines identify files and are not CSV contents. Header markers identify
nodes and relationships, `:LABEL` values supply labels, and `:TYPE` values supply
relationship types. Nodes load before relationship endpoints resolve through
their ID groups. `Person` in `:ID(Person)` names an ID namespace; it does not
add a label. Filenames do not supply labels or relationship types either.
IDs default to `STRING`; numeric-looking data stays string-valued unless the
header declares an integer ID type, such as `{id-type:long}`, or you pass
`--id-type=INTEGER`. Relationship endpoints inherit the referenced ID group's
declared type. Property types come from the headers, not sampled values.

The short command detects comma, pipe, tab or semicolon delimiters from the
headers. If detection reports an ambiguous or oversized header, set the
delimiter explicitly, for example `--delimiter='|'`. Positional paths are
literal by default; opt into matching with `--path-pattern-style=glob` or
`regex` and quote patterns for the shell.

### Inspect files

Inspect the resolved headers and settings before importing:

```sh
velr-admin import graph.db people.csv.gz knows.csv.gz --dry-run
```

`--inspect` is an alias for `--dry-run`. Inspection validates source headers
without creating a database, staging files or a report. It does not scan data
rows or check their property values and relationship endpoints.

### Import manifests

For separate header files, multiple data chunks, or fixed labels and
relationship types, declare the file groups once in a manifest:

```sh
velr-admin import graph.db --manifest import.toml
velr-admin import graph.db --manifest import.toml --dry-run
```

For example, `import.toml` can group data files with separate headers:

```toml
version = 1
delimiter = "|"
id_type = "INTEGER"
ignore_empty_strings = true
bad_tolerance = 0

[[nodes]]
labels = ["Person"]
header = "headers/person.csv"
files = ["data/person_0.csv.gz", "data/person_1.csv.gz"]

[[relationships]]
type = "KNOWS"
header = "headers/knows.csv"
files = ["data/knows_0.csv.gz", "data/knows_1.csv.gz"]
```

Each `files` array must contain data files. Omit `header` when the first file
already has an embedded header. Labels are optional and combine with a row's
`:LABEL` values. Relationships require either a fixed `type` or a `:TYPE`
column. Paths are relative to the manifest's directory; array entries preserve
literal commas in filenames. Manifest paths are literal by default, with
`path_pattern_style` available for explicit matching. CSV settings use
underscore names such as `array_delimiter`,
`multiline_fields` and `skip_bad_relationships`; `memory = "512M"` and
`read_buffer_size = "4M"` configure budgets. Manifest delimiters default to
comma, or set `delimiter = "auto"` for strict detection. Unknown fields and
unsupported versions fail. CLI settings override manifest settings regardless
of argument order, for example `--memory=256M` or `--delimiter=TAB`.
Destination and default report paths remain relative to the working directory.
A manifest cannot be combined with positional CSVs, `--nodes` or
`--relationships`.

### Explicit file groups

Use `--nodes` and `--relationships` to declare file groups directly, with
either `velr-admin import` or `velr-admin database import full`. For example,
these files use separate Neo4j headers:

```text
# people-header.csv
personId:ID(Person)|name:string|active:boolean|tags:string[]|:LABEL

# people-part.csv
1|Aster|true|blue;green|Member
2|Birch|false|red|Reader

# knows-header.csv
:START_ID(Person)|:END_ID(Person)|since:long

# knows-part.csv
1|2|2020
```

Import them with:

```sh
velr-admin database import full graph.db \
  --id-type=INTEGER \
  --delimiter='|' \
  --ignore-empty-strings=true \
  --bad-tolerance=0 \
  --nodes=Person=people-header.csv,people-part.csv \
  --relationships=KNOWS=knows-header.csv,knows-part.csv

velr graph.db -e 'MATCH (a)-[r:KNOWS]->(b) RETURN a.name, b.name, r.since'
```

The `#` lines above identify the example files; they are not CSV contents.
Repeat `--nodes` and `--relationships` for additional groups. Files in one
group are concatenated: the first record supplies the header, and later files
supply data. A header can instead be the first record in a single data file.
Gzip and ZIP input is read directly. ZIP archives must contain one CSV file
using stored or deflated compression. ZIP64 archives are unsupported; use gzip
for large compressed inputs. Command-line `--nodes` and `--relationships`
filename patterns use regular expressions by default, as in Neo4j; use
`--path-pattern-style=glob` for shell-style globs.
Quote a pattern to prevent the shell from expanding it before the importer:

```sh
velr-admin database import full graph.db \
  --nodes='Person=people-header.csv,person-part-.*[.]csv[.]gz'
```

ID groups are independent, so `:ID(Person)` and `:ID(Message)` may reuse the
same values. A named ID column such as `personId:ID(Person)` also becomes a
property; an anonymous `:ID(Person)` is only used to resolve endpoints. Named
IDs use strings by default and integers with `--id-type=INTEGER` or an explicit
`{id-type:long}` header declaration. `--id-type=ACTUAL` uses supplied IDs
directly as internal node IDs and does not create ID properties, even for
named ID columns. Labels in `--nodes=Message:Post=...` are combined with a
row's `:LABEL` values. Use a
fixed relationship type in `--relationships=KNOWS=...`, or supply a `:TYPE`
column. `:IGNORE` columns are discarded.

### CSV options

The long command's default CSV delimiter is `,`; use `--delimiter=auto` to opt
into the short command's delimiter detection. The array and label delimiter
defaults to `;`, and the quote character is `"`. Typed headers include
`string`, integer types, floating-point types and `boolean`, with `[]` for
arrays. CSV quoting preserves delimiters and escaped double quotes inside a
field. Set `--multiline-fields=true` for quoted fields spanning multiple lines.
Unquoted empty fields are absent properties; quoted empty strings are retained
unless `--ignore-empty-strings=true`.

`--normalize-types=true` is the default: integer types become Cypher integers
and floating-point types become doubles. Set `--normalize-types=false` to
preserve the declared numeric parsing ranges and float precision. Velr stores
the resulting numeric values as `i64` and `f64` in either mode.

### Argument files

Long commands can put the destination and options into an argument file:

```text
# import.args
graph.db
--id-type=INTEGER
--delimiter='|'
--nodes='Person=people-header.csv,people-part.csv'
--relationships='KNOWS=knows-header.csv,knows-part.csv'
```

```sh
velr-admin database import full @import.args
```

On Windows, use `--option` switches and quote paths containing spaces. Both
`C:\data\people.csv` and `C:/data/people.csv` are supported, as are UNC paths
such as `\\server\share\people.csv`. Windows argument files preserve path
backslashes, including inside double quotes:

```text
"C:\data dir\graph.db"
--nodes="Person=C:\data dir\people.csv"
```

In TOML manifests, use single-quoted strings for paths containing backslashes,
for example `files = ['C:\data\people.csv']`, or use forward slashes. TOML
double-quoted strings require each backslash to be escaped as `\\`.

### Import completion and diagnostics

The importer builds a temporary database beside the destination and publishes
it after building indexes, collecting planner statistics, and validating
database integrity and relationship references. These final steps can take
time after all CSV rows have loaded.

A failed import leaves the destination intact and removes temporary database
files. Existing destinations require
`--overwrite-destination=true`; close all users of the destination first.
Diagnostics go to `import.report` by default, configurable with `--report-file`.
Duplicate IDs and missing endpoints fail by default. Use
`--skip-duplicate-nodes=true` or `--skip-bad-relationships=true` to skip those
entries, with `--bad-tolerance` setting the maximum number that may be skipped.
Malformed headers and property values always fail with their source location.

The completion summary reports node, relationship and property counts, skipped
entries, and total elapsed time. Open the completed database with `velr`:

```sh
velr graph.db -e 'MATCH (p:Person) RETURN count(p) AS people'
```

### Memory and input limits

`--memory` (also called `--max-off-heap-memory`) sets the budget for importer
caches and working buffers. The default is `512M`. Use an explicit size such
as `256M` or `1G`. On macOS and Linux, you can also use a percentage of
available memory such as `75%`. Total process memory can exceed this budget.

The importer processes large datasets using temporary files when needed;
it does not keep the entire database in RAM. Allow disk space beside the
destination for both the database and temporary import files.

`--read-buffer-size` sets the input buffer and maximum CSV field size. It
defaults to `4M`; increase it when a field exceeds that limit:

```sh
velr-admin import graph.db people.csv.gz knows.csv.gz \
  --memory=1G --read-buffer-size=8M
```

Input must be local UTF-8 CSV. Parquet, cloud inputs and Neo4j engine/storage
configuration are unsupported. Floating-point values must be finite;
`NaN` and `Infinity` are rejected.

Run `velr-admin --help` for the complete list of options.

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

- `--explain '<query>'` prints a compact explain trace without executing
  the query.
- `-e 'EXPLAIN ...'` or `-e 'EXPLAIN ANALYZE ...'` renders the statement
  results in the selected output format.

`EXPLAIN ANALYZE` executes the query to collect timings.

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

Building from source requires a Rust toolchain. Clone the public repository
and build both executables:

```sh
git clone https://github.com/velr-ai/velr.git
cd velr/rust/velr-cli
cargo build --release --bins
cargo run --release --bin velr -- --version
cargo run --release --bin velr-admin -- --help
```

The executables are written to `target/release` (with `.exe` on Windows).
To build a specific release, check out its Git tag before building.

Exit Codes
----------

- `0`: success
- `1`: query or runtime error
- `2`: usage error

`velr-admin` returns `1` for both import failures and argument errors.

License
-------

The CLI source is licensed under MIT. Prebuilt binary archives also include the
Velr runtime binary redistribution license.
