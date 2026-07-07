# Velr

## What This Page Is

This repository is the public landing page for the Velr project.

Start here if you want to understand what Velr is, what is available today,
where the public packages live, how to try it, and where to ask questions or
report issues. This page is intentionally an umbrella page, not a full API
reference. For driver-level details, follow the language-specific links below.

We would love to hear what you are building with Velr and what would make it
more useful.

## What Velr Is

Velr is an embedded property-graph database from Velr.ai, implemented in Rust,
stored in a standard SQLite database file, and queried with **openCypher**.
It also supports full-text BM25 search and vector approximate nearest neighbor
(ANN) search for retrieval-heavy graph applications.

It runs in-process instead of as a separate database server. That makes it a
good fit for applications that need graph queries close to their data:
local-first software, edge and physical AI systems, agent memory, data products,
and modern Rust, Python, Go, Java, Kotlin, JavaScript, and TypeScript workflows.

Velr is available today through public Rust, Python, Go, Java, Kotlin,
JavaScript, and TypeScript drivers. Each driver wraps a bundled native runtime
implemented in Rust, so applications can use Velr without running a separate
service.

## Public Resources

- Website: [velr.ai](https://velr.ai/)
- Community questions: [GitHub Discussions](https://github.com/velr-ai/velr/discussions)
- Bug reports and feature requests: [GitHub Issues](https://github.com/velr-ai/velr/issues)

### Command Line Shell

The Velr CLI (`velr`) is an interactive shell and scripting tool for running
Cypher queries, opening local Velr database files, and inspecting query plans
from a terminal.

Install with Homebrew on macOS, Linux, or Windows Subsystem for Linux (WSL):

```sh
brew tap velr-ai/velr
brew install velr
```

If Homebrew asks you to trust the third-party formula first:

```sh
brew trust --formula velr-ai/velr/velr
brew install velr
```

Run a query and exit:

```sh
velr -e 'MATCH (n) RETURN n LIMIT 5'
```

Open an interactive shell on a database file:

```sh
velr graph.db
```

For shell usage, release archives, and build-from-source instructions, see the
[Velr CLI README](rust/velr-cli/README.md).

### Language Drivers

Use the language-specific drivers when embedding Velr directly in an
application.

#### Rust

- Crate: [velr on crates.io](https://crates.io/crates/velr)
- API docs: [velr on docs.rs](https://docs.rs/velr/latest/velr/)
- Driver repository: [velr-rust-driver](https://github.com/velr-ai/velr-rust-driver)
- Examples: [velr-rust-examples](https://github.com/velr-ai/velr-rust-examples)

#### Python

- Package: [velr on PyPI](https://pypi.org/project/velr/)
- Examples: [velr-python-examples](https://github.com/velr-ai/velr-python-examples)

#### Go

- Module: [velr-go-driver](https://github.com/velr-ai/velr-go-driver)
- API docs: [velr-go-driver on pkg.go.dev](https://pkg.go.dev/github.com/velr-ai/velr-go-driver)
- Examples: [velr-go-examples](https://github.com/velr-ai/velr-go-examples)

#### Java

- Maven Central JVM artifact: [velr-java-driver](https://central.sonatype.com/artifact/ai.velr/velr-java-driver)
- Maven Central Android artifact: [velr-java-driver-android](https://central.sonatype.com/artifact/ai.velr/velr-java-driver-android)
- JVM API docs: [Java JVM docs](https://velr-ai.github.io/velr-java-driver/docs/jvm/)
- Android API docs: [Java Android docs](https://velr-ai.github.io/velr-java-driver/docs/android/)
- Driver repository: [velr-java-driver](https://github.com/velr-ai/velr-java-driver)
- Examples: [velr-java-examples](https://github.com/velr-ai/velr-java-examples)

#### Kotlin

- Maven Central JVM artifact: [velr-kotlin-driver](https://central.sonatype.com/artifact/ai.velr/velr-kotlin-driver)
- Maven Central Android artifact: [velr-kotlin-driver-android](https://central.sonatype.com/artifact/ai.velr/velr-kotlin-driver-android)
- JVM API docs: [Kotlin JVM docs](https://velr-ai.github.io/velr-kotlin-driver/docs/jvm/)
- Android API docs: [Kotlin Android docs](https://velr-ai.github.io/velr-kotlin-driver/docs/android/)
- Driver repository: [velr-kotlin-driver](https://github.com/velr-ai/velr-kotlin-driver)
- Examples: [velr-kotlin-examples](https://github.com/velr-ai/velr-kotlin-examples)

#### JavaScript / TypeScript

- Package: [@velr-ai/velr on npm](https://www.npmjs.com/package/@velr-ai/velr)
- JavaScript examples: [velr-javascript-examples](https://github.com/velr-ai/velr-javascript-examples)
- TypeScript examples: [velr-typescript-examples](https://github.com/velr-ai/velr-typescript-examples)

## Status

Velr is currently in **public alpha**.

- The public driver APIs are usable, but still evolving.
- The current public drivers are in the `0.2.x` series.
- Velr supports openCypher and passes all positive openCypher TCK tests. Exact
  error semantics are not guaranteed to match other openCypher implementations.
- Full-text BM25 search and vector approximate nearest neighbor (ANN) search
  are available today through Cypher DDL and `CALL` syntax. Their APIs may
  still evolve while Velr remains alpha.

Velr is already useful for real workflows and representative use cases, but you
should expect rough edges while the project moves toward a stable 1.0 release.

## Feature Snapshot

- Embedded graph database runtime backed by SQLite
- In-memory and file-backed databases
- openCypher query execution
- Rust, Python, Go, Java, Kotlin, JavaScript, and TypeScript public drivers
  with bundled native runtimes
- Query parameter binding in the public drivers
- Transactions and savepoints
- Read-only database opening for viewers, agents, and inspection tools
- Explicit database migration support
- Observed graph-shape introspection with `SHOW CURRENT GRAPH SHAPE`
- Full-text BM25 indexes with `CREATE FULLTEXT INDEX`
- Vector approximate nearest neighbor (ANN) indexes with `CREATE VECTOR INDEX`
  and application-provided embedders
- Result streaming and bounded previews
- Arrow IPC support, including Python interop with PyArrow, pandas, and Polars,
  Go/JavaScript/TypeScript interop with Apache Arrow, and Java/Kotlin Arrow IPC
  and Arrow C Data bindings

## Quickstart

Choose the driver that fits your application. The examples below create an
in-memory graph, bind a parameter, and read one result.

### Python

Install the Python package from PyPI:

```sh
python -m pip install velr
```

Python 3.12 or newer is required.

```python
from velr.driver import Velr

with Velr.open(None) as db:
    db.run(
        "CREATE (:Person {name: $name})",
        params={"name": "Ada Lovelace"},
    )

    with db.exec_one("MATCH (p:Person) RETURN p.name AS name") as table:
        rows = table.collect(lambda row: [cell.as_python() for cell in row])
        print(rows)
```

Use `Velr.open("graph.db")` for a file-backed database.

### Go

Install the Go module:

```sh
go get github.com/velr-ai/velr-go-driver@latest
```

Go 1.22 or newer is required.

```go
package main

import (
	"fmt"
	"log"

	velr "github.com/velr-ai/velr-go-driver"
)

func main() {
	db, err := velr.OpenInMemory()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.RunWithParams("CREATE (:Person {name: $name})", velr.Params{
		"name": "Ada Lovelace",
	})
	if err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query("MATCH (p:Person) RETURN p.name AS name")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(rows)
}
```

Use `velr.Open("graph.db")` for a file-backed database.

### Java

Add the JVM artifact from Maven Central:

```kotlin
repositories {
    mavenCentral()
}

dependencies {
    implementation("ai.velr:velr-java-driver:0.2.35")
}
```

For Android applications, use
`implementation("ai.velr:velr-java-driver-android:0.2.35")`.

```java
import ai.velr.QueryOptions;
import ai.velr.Velr;
import java.util.List;
import java.util.Map;

try (Velr db = Velr.open()) {
    db.run(
        "CREATE (:Person {name: $name})",
        QueryOptions.builder().param("name", "Ada Lovelace").build());

    List<Map<String, Object>> rows =
        db.query("MATCH (p:Person) RETURN p.name AS name");
    System.out.println(rows);
}
```

Use `Velr.open("graph.db")` for a file-backed database.

### Kotlin

Add the JVM artifact from Maven Central:

```kotlin
repositories {
    mavenCentral()
}

dependencies {
    implementation("ai.velr:velr-kotlin-driver:0.2.35")
}
```

For Android applications, use
`implementation("ai.velr:velr-kotlin-driver-android:0.2.35")`.

```kotlin
import ai.velr.QueryOptions
import ai.velr.velr

velr().use { db ->
    db.run(
        "CREATE (:Person {name: $name})",
        QueryOptions.builder().param("name", "Ada Lovelace").build(),
    )

    val rows = db.query("MATCH (p:Person) RETURN p.name AS name")
    println(rows)
}
```

Use `velr("graph.db")` for a file-backed database.

### JavaScript / TypeScript

Install the Node.js package from npm:

```sh
npm install @velr-ai/velr
```

Node.js 22 or newer is required.

```ts
import { Velr } from "@velr-ai/velr";

const db = Velr.open(null);
try {
  db.run("CREATE (:Person {name: $name})", {
    params: { name: "Ada Lovelace" },
  });

  const rows = db.query(
    "MATCH (p:Person) RETURN p.name AS name",
    { int64: "number" }
  );
  console.log(rows);
} finally {
  db.close();
}
```

Use `Velr.open("graph.db")` for a file-backed database.

### Rust

Add the Rust crate:

```toml
[dependencies]
velr = "0.2"
```

```rust
use velr::{CellRef, Velr};

fn main() -> velr::Result<()> {
    let db = Velr::open(None)?;

    db.run_with_params(
        "CREATE (:Person {name: $name})",
        velr::params! { name: "Ada Lovelace" }?,
    )?;

    let mut table = db.exec_one("MATCH (p:Person) RETURN p.name AS name")?;
    table.for_each_row(|row| {
        if let CellRef::Text(name) = row[0] {
            println!("{}", std::str::from_utf8(name).unwrap());
        }
        Ok(())
    })?;

    Ok(())
}
```

Use `Velr::open(Some("graph.db"))` for a file-backed database.

## Roadmap Direction

The main path to Velr 1.0 is stabilization: clearer error behavior, stable
public APIs, and better documentation.

Vector approximate nearest neighbor (ANN) search, full-text BM25 search,
graph-shape introspection, parameter binding, transactions, and data-frame/Arrow
interop are already present and will continue to harden. Longer-term directions
include time-series and federation.

## License

The public driver source packages are licensed under MIT. The bundled native
runtime binaries may be used and freely redistributed in unmodified form under
the Velr Free Binary Redistribution License (`LICENSE.runtime` in each package).
See the package license files for the full terms.
