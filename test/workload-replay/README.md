# Workload Capture & Replay

Tools for capturing, anonymizing, and replaying production workloads from live Materialize instances. This enables realistic performance testing, benchmarking, and regression detection in a controlled local environment, with either synthetic or real captured data.

## Overview

There are three main components:

1. **Capture** (`mz-workload-capture`) - Records schema definitions, cluster configurations, query history, and optionally actual data from a live Materialize instance
2. **Anonymize** (`mz-workload-anonymize`) - Removes sensitive identifiers from captured workloads for safe sharing
3. **Replay** (`bin/mzcompose --find workload-replay`) - Simulates captured workloads locally using Docker Compose

## Quick Start

```bash
# Capture a workload (metadata only, single YAML file)
bin/mz-workload-capture postgres://mz_system:materialize@127.0.0.1/materialize \
  --output workload_myapp \
  --time 3600

# Capture a workload WITH actual data (directory with TSV files)
bin/mz-workload-capture postgres://mz_system:materialize@127.0.0.1/materialize \
  --capture-data \
  --output workload_myapp \
  --time 3600

# Anonymize for sharing (metadata-only captures)
bin/mz-workload-anonymize workload_myapp.yml -o workload_myapp_anon.yml

# Run the workload locally, uses files in test/workload-replay/captured-workloads
# https://github.com/MaterializeInc/captured-workloads
bin/mzcompose --find workload-replay run default workload_myapp_anon.yml

# Run a data-captured workload (pass the directory)
bin/mzcompose --find workload-replay run default workload_myapp/

# Benchmark against another version
bin/mzcompose --find workload-replay run benchmark workload_myapp_anon.yml --compare-against v26.9.0
```

## Components

### Workload Capture

Captures the current state and activity of a live Materialize instance.

**What it captures (always):**
- Schema definitions (databases, schemas, tables, views, materialized views, indexes, sources, sinks, connections, types)
- Cluster configurations
- Recent query history from `mz_internal.mz_recent_activity_log`
- Source ingestion statistics (messages/bytes rates and totals)
- Optionally sampled column size statistics (expensive to fetch, `--avg-column-size`)

**What it captures (with `--capture-data`):**
- Initial data snapshot via `COPY (SELECT * FROM ...) TO STDOUT` for each source/subsource/table
- Continuous data changes via `SUBSCRIBE ... WITH (SNAPSHOT = false, PROGRESS = true)` for the `--time` duration
- Data is stored as TSV files (PostgreSQL COPY text format) with accompanying `.meta.json` metadata

**Usage:**
```bash
bin/mz-workload-capture <mz_url> [OPTIONS]
```

**Options:**
| Option | Description | Default |
|--------|-------------|---------|
| `-o, --output` | Path to write the workload (YAML file or directory) | `workload_<timestamp>` |
| `--time` | Duration of query/ingestion history in seconds | 360 |
| `--capture-data` | Capture actual data via SQL into TSV files | disabled |
| `--max-subscribe-connections` | Max concurrent SUBSCRIBE connections | 32 |
| `--avg-column-size` | Enable expensive column size calculations | disabled |
| `-v, --verbose` | Verbose output | disabled |

**Examples:**
```bash
# Metadata-only capture (writes workload_prod.yml)
bin/mz-workload-capture postgres://mz_system:materialize@127.0.0.1/materialize \
  --output workload_prod \
  --time 7200 \
  --verbose

# Capture with actual data (writes workload_prod/ directory)
bin/mz-workload-capture postgres://mz_system:materialize@127.0.0.1/materialize \
  --capture-data \
  --output workload_prod \
  --time 600 \
  --max-subscribe-connections 16
```

### Data Capture Directory Structure

When `--capture-data` is used, the output is a directory:

```
workload_<timestamp>/
├── objects.yml          # Schema metadata, cluster configs, source statistics
├── queries.yml          # Captured query history
├── initial_data/        # Snapshot data (COPY TO STDOUT output)
│   ├── materialize.public.my_table.tsv
│   ├── materialize.public.my_table.meta.json
│   ├── materialize.public.orders.tsv
│   └── materialize.public.orders.meta.json
└── continuous_data/     # SUBSCRIBE output (with timestamps)
    ├── materialize.public.my_table.tsv
    ├── materialize.public.my_table.meta.json
    └── ...
```

**TSV Format:**
- Initial data: Standard PostgreSQL COPY text format (tab-delimited, `\N` for NULLs)
- Continuous data: Same format with three prepended columns: `mz_timestamp`, `mz_progressed`, `mz_diff`

**meta.json Format:**
```json
{
  "database": "materialize",
  "schema": "public",
  "name": "orders",
  "source_type": "postgres",
  "parent_source_fqn": "materialize.public.pg_source",
  "columns": [
    {"name": "id", "type": "integer", "nullable": false},
    {"name": "amount", "type": "numeric", "nullable": true}
  ],
  "row_count": 12345
}
```

The `source_type` field determines how data is routed during replay:
- `table` → `COPY FROM STDIN` into Materialize table
- `postgres` → `COPY FROM STDIN` into upstream Postgres DB
- `mysql` → batch `INSERT` into upstream MySQL DB
- `sql-server` → `INSERT` via testdrive into upstream SQL Server DB
- `kafka` → produce Avro messages to Kafka topic
- `webhook` → HTTP POST to webhook endpoint

### Workload Anonymization

Anonymizes identifiers and literals in workload captures for sharing without exposing sensitive information.

**What it anonymizes:**

*Identifiers (`--identifiers`, enabled by default):*
- Database names → `db_0`, `db_1`, ...
- Schema names → `schema_1`, `schema_2`, ...
- Table names → `table_1`, `table_2`, ...
- Column names → `column_1`, `column_2`, ...
- View, materialized view, source, sink, connection names
- All identifiers in `create_sql` definitions and queries

*Literals (`--literals`, enabled by default):*
- String literals in SQL → `'literal_1'`, `'literal_2'`, ...
- String default values in table/source/child columns
- String literals in queries

**Usage:**
```bash
bin/mz-workload-anonymize <file> [OPTIONS]
```

**Options:**
| Option | Description | Default |
|--------|-------------|---------|
| `-o, --output` | Path to write output | overwrites input file |
| `--identifiers` / `--no-identifiers` | Anonymize object names | enabled |
| `--literals` / `--no-literals` | Anonymize string literals | enabled |

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

Note that the query and `create-sql` replacements are currently heuristics and can go wrong. If possible, share an unanonymized workload yaml file.

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

# Run a data-captured workload
bin/mzcompose --find workload-replay run test workload_prod/

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

2. Initial data
   - If SQL-captured: import TSV data into external systems and Materialize tables
   - If synthetic: generate random data matching captured schemas and statistics
   - Wait for hydration of all objects

3. Continuous Phase (runs in parallel)
   - If SQL-captured: replay SUBSCRIBE data at original cadence (scaled by --factor-ingestions)
   - If synthetic: generate data at original ingestion rates (scaled by --factor-ingestions)
   - Replay queries at original timing (scaled by --factor-queries)
   - Collect Docker stats (CPU, memory, disk)

4. Metrics & Reporting (in `benchmark` workflow)
   - Print timing statistics (avg, median, p95, p99)
   - Generate comparison tables
   - Generate CPU/memory plots
   - Identify performance regressions
```

## Workload File Format

### Single-file format (metadata only)

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

### Directory format (with captured data)

When `--capture-data` is used, the workload is split into `objects.yml` (same as above minus queries, plus `capture_method: sql`) and `queries.yml`, alongside `initial_data/` and `continuous_data/` directories containing TSV and meta.json files.

## Data Generation

### Synthetic (default)

The framework generates realistic random data based on captured schema information:

- **Type-aware generation** for all SQL types (INT, TEXT, TIMESTAMP, JSONB, etc.)
- **Long-tail distribution** for realistic data patterns (Pareto distribution)
- **Configurable seed** for reproducibility
- **Column-level statistics** from capture inform data shape

### SQL-captured (with `--capture-data`)

Real data is captured from the live instance and replayed verbatim:

- **Initial data** is loaded into external systems (Postgres, MySQL, Kafka, SQL Server) or Materialize tables before the continuous phase
- **Continuous data** is replayed at the original timestamps, scaled by `--factor-ingestions`
- Only inserts (`mz_diff=1`) from SUBSCRIBE output are replayed

Scaling factors control the workload intensity:
- `--factor-initial-data` scales the number of rows generated (synthetic only)
- `--factor-ingestions` scales ingestion rates / replay speed
- `--factor-queries` scales query frequency
