# Workload Capture & Replay

Tools for capturing, anonymizing, and replaying production workloads from live Materialize instances. This enables realistic performance testing, benchmarking, and regression detection in a controlled local environment, with synthetic source data.

## Overview

There are three main components:

1. **Capture** (`mz-workload-capture`) - Records schema definitions, cluster configurations, and query history from a live Materialize instance
2. **Anonymize** (`mz-workload-anonymize`) - Removes sensitive identifiers from captured workloads for safe sharing
3. **Replay** (`bin/mzcompose --find workload-replay`) - Simulates captured workloads locally using Docker Compose

## Quick Start

```bash
# Capture a workload from a running Materialize instance
bin/mz-workload-capture postgres://mz_system:materialize@127.0.0.1/materialize \
  --output workload_myapp.yml \
  --time 3600

# Anonymize for sharing
bin/mz-workload-anonymize workload_myapp.yml -o workload_myapp_anon.yml

# Run the workload locally, uses files in test/workload-replay/captured-workloads
# https://github.com/MaterializeInc/captured-workloads
bin/mzcompose --find workload-replay run default workload_myapp_anon.yml

# Benchmark against another version
bin/mzcompose --find workload-replay run benchmark workload_myapp_anon.yml --compare-against v26.9.0
```

## Components

### Workload Capture

Captures the current state and activity of a live Materialize instance without actual data.

**What it captures:**
- Schema definitions (databases, schemas, tables, views, materialized views, indexes, sources, sinks, connections, types)
- Cluster configurations
- Recent query history from `mz_internal.mz_recent_activity_log`
- Source ingestion statistics (messages/bytes rates and totals)
- Optionally sampled column size statistics (expensive to fetch, `--avg-column-size`)

**Usage:**
```bash
bin/mz-workload-capture <mz_url> [OPTIONS]
```

**Options:**
| Option | Description | Default |
|--------|-------------|---------|
| `-o, --output` | Path to write workload YAML file, `-` for stdout | `workload_<timestamp>.yml` |
| `--time` | Duration of query/ingestion history in seconds | 360 |
| `--avg-column-size` | Enable expensive column size calculations | disabled |
| `-v, --verbose` | Verbose output | disabled |

**Example:**
```bash
bin/mz-workload-capture postgres://mz_system:materialize@127.0.0.1/materialize \
  --output captured-workloads/workload_prod.yml \
  --time 7200 \
  --verbose
```

### Workload Anonymization

Anonymizes identifiers and literals in workload captures for sharing without exposing sensitive information.

**What it anonymizes:**

*Identifiers (`--identifiers`, enabled by default):*
- Database names → `db_0`, `db_1`, ...
- Schema names → `schema_1`, `schema_2`, ...
- Table names → `table_1`, `table_2`, ...
- Column names → `column_1`, `column_2`, ...
- View, materialized view, source, sink, connection names
- All identifier references in `create_sql` definitions and queries

Identifier renaming is done on the parsed AST by `mz-sql-anonymize`, which
renames whole identifier tokens. A text regex (the previous approach) corrupted
SQL by matching identifiers as substrings or inside string literals; the AST
does not.

*Literals (`--literals`, enabled by default):*
All literal redaction is done on the AST by `mz-sql-anonymize`:
- **Query string literals** become the inert `'<REDACTED>'` placeholder (which
  replay binds as `NULL`). **DDL string values** (a topic, an upstream database,
  an external reference, a connection host/user) are instead renamed
  *consistently* to a stable `redacted_<hash>` token, so the same value lines up
  everywhere replay cross-references it; a value that is a renamed catalog object
  takes that object's anonymized name. Inline Avro schemas are anonymized in
  place (field names follow the column renaming; upstream namespaces scrubbed) so
  they stay valid and replayable.
- **Numbers** are redacted (to the neutral literal `1`) where they are data: in
  queries and in view/materialized-view/index **bodies** (`WHERE ssn =
  123456789`). They are **kept** in option positions — cluster sizes, ports,
  replication factors, numeric column defaults — which are config replay needs
  valid. Hex strings are redacted; **intervals/durations** (`'1s'`, `INTERVAL
  '60' DAY`, `date_trunc('year', …)`) are kept as non-sensitive config.
- Built-in identifiers are never renamed: system-schema/-catalog names
  (`pg_catalog`, `mz_internal`, and the columns/functions/types they expose) and
  load-generator references/columns (`mz_load_generators.auction.accounts`,
  `l_quantity`), so views/queries over them still resolve on replay.
- The `redacted_<hash>` tokens are salted with a fresh per-run random value, so
  they stay consistent within one anonymized file but cannot be reversed by a
  dictionary attack on the shared output.
- Bound query parameters (the `$1, $2, ...` values a prepared statement ran
  with) live alongside the SQL, not in it; each is redacted to `<REDACTED>`,
  which replay binds as `NULL`.
- Cluster sizing/replication and session/system config (`SET`/`RESET`/`ALTER
  SYSTEM`, e.g. timeouts) are **preserved** — replay needs them and they are not
  sensitive.

**The parser binary is required.** Anonymization runs entirely on Materialize's
parsed AST, so the helper must be built. If it is missing, or a statement does
not parse, the tool refuses to write rather than risk leaking. Build it once:
```bash
cargo build --release -p mz-sql-anonymize
```

**Usage:**
```bash
bin/mz-workload-anonymize <file> [OPTIONS]
```

**Options:**
| Option | Description | Default |
|--------|-------------|---------|
| `-o, --output` | Path to write output (`-` for stdout); required unless `--in-place` | — |
| `--in-place` | Overwrite the input file (destroys the original capture) | off |
| `--identifiers` / `--no-identifiers` | Anonymize object names | enabled |
| `--literals` / `--no-literals` | Anonymize literals | enabled |
| `--verify` / `--no-verify` | Re-scan output for leaks and refuse to write if any are found | enabled |

**Examples:**
```bash
# Full anonymization (default)
bin/mz-workload-anonymize workload_prod.yml -o workload_prod_anon.yml

# Only anonymize identifiers, keep original string literals
bin/mz-workload-anonymize workload_prod.yml --no-literals -o workload_prod_anon.yml

# Only anonymize literals, keep original object names
bin/mz-workload-anonymize workload_prod.yml --no-identifiers -o workload_prod_anon.yml
```

The anonymizer preserves SQL keywords and built-in Materialize objects while applying consistent mapping throughout the workload to maintain referential integrity.

Identifier renaming and literal redaction are done on the parsed AST, so they no
longer corrupt SQL the way the previous text-regex approach could. If a workload
is not sensitive, sharing it unanonymized still gives the most faithful replay.

### Workload Replay

Simulates captured workloads locally using Docker Compose, recreating the schema and replaying queries with configurable scaling.

**Workflows:**

#### Test Workflow
Runs the workload and reports metrics.

```bash
bin/mzcompose --find workload-replay run default [OPTIONS] [files]
```

#### Benchmark Workflow
Compares performance between two Materialize versions.

```bash
bin/mzcompose --find workload-replay run benchmark [OPTIONS] [files]
```

#### Stats Workflow
Prints statistics about captured workloads without running them.

```bash
bin/mzcompose --find workload-replay run stats [files]
```

**Options:**
| Option | Description | Default |
|--------|-------------|---------|
| `--factor-initial-data` | Scale factor for initial data size | 1.0 |
| `--factor-ingestions` | Scale factor for ingestion rates | 1.0 |
| `--factor-queries` | Scale factor for query frequency | 1.0 |
| `--runtime` | Duration of continuous phase in seconds | 1200 |
| `--max-concurrent-queries` | Maximum concurrent queries | 1000 |
| `--seed` | Random seed for data generation | random |
| `--verbose` | Verbose output | disabled |
| `--create-objects` | Create schema objects | true |
| `--initial-data` | Create initial data | true |
| `--early-initial-data` | Create initial data before sources | false |
| `--run-ingestions` | Run continuous ingestions | true |
| `--run-queries` | Run continuous queries | true |
| `--compare-against` | Materialize version to compare against (benchmark only) | none |
| `--skip-without-data-scale` | Skip workloads that have `scale_data: false` in their settings (benchmark only) | false |

**Examples:**
```bash
# Run with reduced load for quick testing
bin/mzcompose --find workload-replay run test workload_prod.yml \
  --factor-queries 0.1 \
  --runtime 300

# Benchmark with custom scaling
bin/mzcompose --find workload-replay run benchmark workload_prod.yml \
  --compare-against v0.89.0 \
  --factor-initial-data 0.5 \
  --factor-ingestions 0.5 \
  --runtime 600

# Run multiple workloads
bin/mzcompose --find workload-replay run test workload_a.yml workload_b.yml

# Run all workloads in captured-workloads/
bin/mzcompose --find workload-replay run test

# Stop the running Docker Compose setup
bin/mzcompose --find workload-replay down
```

## Replay Execution Phases

The replay executes in four phases:

```
1. Initialization
   - Start Docker services (materialized, kafka, postgres, etc.)
   - Set up Materialize system parameters
   - Create clusters, databases, schemas
   - Create types, connections, sources, sinks, tables, views, indexes

2. Initial data generation
   - Generate data for external sources (postgres, mysql, sql server, kafka)
   - Populate Materialize tables via `COPY`
   - Ingest initial webhook data
   - Wait for hydration of all objects

3. Continuous Phase (runs in parallel)
   - Replay ingestions at original rates (scaled by `--factor-ingestions`)
   - Replay queries at original timing (scaled by `--factor-queries`)
   - Collect Docker stats (CPU, memory, disk)

4. Metrics & Reporting (in `benchmark` workflow)
   - Print timing statistics (avg, median, p95, p99)
   - Generate comparison tables
   - Generate CPU/memory plots
   - Identify performance regressions
```

## Workload File Format

Captured workloads are stored as YAML files with the following structure:

```yaml
mz_workload_version: "1.0.0"

databases:
  <db_name>:
    <schema_name>:
      tables:
        <table_name>:
          create_sql: "CREATE TABLE ..."
          columns: [...]
          rows: 1000
      views: { ... }
      materialized_views: { ... }
      sources: { ... }
      sinks: { ... }
      connections: { ... }
      indexes: { ... }
      types: { ... }

clusters:
  <cluster_name>:
    create_sql: "CREATE CLUSTER ..."
    managed: true

settings:
  scale_data: true  # When false, --factor-initial-data is ignored (uses 100%)

queries:
  - sql: "SELECT * FROM ..."
    cluster: "default"
    database: "materialize"
    search_path: ["public"]
    statement_type: "select"
    finished_status: "success"
    params: []
    duration: 0.123
    result_size: 100
```

## Synthetic Data Generation

The framework tries to generate realistic random data based on captured schema information:

- **Type-aware generation** for all SQL types (INT, TEXT, TIMESTAMP, JSONB, etc.)
- **Long-tail distribution** for realistic data patterns (Pareto distribution)
- **Configurable seed** for reproducibility
- **Column-level statistics** from capture inform data shape

Scaling factors control the workload intensity:
- `--factor-initial-data` scales the number of rows generated
- `--factor-ingestions` scales ingestion rates (messages/second)
- `--factor-queries` scales query frequency
