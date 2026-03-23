# persist-shared-log

A group commit consensus service for Materialize persist. Batches independent
cross-shard CAS writes into a single durable S3 Express One Zone PUT per flush
interval, making cost O(1/batch_window) instead of O(shards).

## Architecture

```
environmentd ──gRPC──▶ consensus-svc (single-threaded actor)
                              │
                              ▼
                        S3 Express One Zone
                        ├── consensus/wal/00000000000000000001
                        ├── consensus/wal/00000000000000000002
                        └── consensus/snapshot
```

All shard state lives in-memory in a single-threaded actor. Writes are buffered
and flushed to S3 as a single WAL batch every flush interval (default 20ms).
Snapshots are written periodically for faster recovery.

## Prerequisites

- CockroachDB running locally (for timestamp oracle and other metadata)
- AWS credentials with access to an S3 Express One Zone directory bucket
- Standard Materialize dev setup (`bin/environmentd` must work)

## Running

### 1. Start the consensus service

```bash
AWS_PROFILE=mz-scratch-admin cargo run -p mz-persist-shared-log -- \
  --s3-bucket <your-s3-express-bucket> \
  --s3-prefix consensus/ \
  --s3-region us-east-1
```

The service listens on `0.0.0.0:6890` by default.

For local development with LocalStack/MinIO, pass `--s3-endpoint`:

```bash
cargo run -p mz-persist-shared-log -- \
  --s3-bucket test-bucket \
  --s3-prefix consensus/ \
  --s3-endpoint http://localhost:4566
```

### 2. Start environmentd

```bash
./bin/environmentd --reset -- \
  --persist-consensus-url='rpc://localhost:6890' \
  --system-parameter-default=default_timestamp_interval=100ms
```

The `--reset` flag clears previous state. Omit it on subsequent runs to keep
data across restarts. The `--system-parameter-default` flag sets the timestamp
interval to 100ms (default 1s), which controls how frequently tables and sources
commit data through the consensus service.

### 3. Connect

```bash
psql postgres://materialize@localhost:6875/materialize
```

## CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `--listen-addr` | `0.0.0.0:6890` | gRPC listen address |
| `--s3-bucket` | (required) | S3 bucket for WAL and snapshot storage |
| `--s3-prefix` | `consensus/` | Key prefix for all S3 objects |
| `--s3-endpoint` | (none) | S3 endpoint override for LocalStack/MinIO |
| `--s3-region` | `us-east-1` | AWS region |
| `--flush-interval-ms` | `20` | How often to flush buffered writes to S3 |
| `--snapshot-interval` | `100` | Write a snapshot every N WAL batches |

## Tuning

### Flush interval

The flush interval controls the trade-off between write latency and S3 cost.
Each flush produces one S3 PUT regardless of how many shards wrote.

### Source/table timestamp interval

By default, sources and tables advance timestamps every 1s. This can be set at
startup via `--system-parameter-default=default_timestamp_interval=100ms` (as
shown above), or changed at runtime:

```sql
ALTER SYSTEM SET default_timestamp_interval = '100ms';
```

Or per-source:

```sql
ALTER SOURCE my_src SET (TIMESTAMP INTERVAL = '100ms');
```
