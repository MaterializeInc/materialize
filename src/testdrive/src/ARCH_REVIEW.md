# ARCH_REVIEW: mz-testdrive

## Finding 1 — Production catalog dependency in test infrastructure

**Location:** `src/testdrive/Cargo.toml` + `src/testdrive/src/action/consistency.rs`

`mz-testdrive` depends on `mz-adapter` and `mz-catalog` (not just a SQL client)
because `consistency.rs` calls into internal catalog and coordinator APIs to
run post-file invariant checks. This couples the test runner's build to the full
production Adapter stack. Any internal refactor to `mz-catalog` schema or
`mz-adapter` API surfaces can break testdrive compilation independently of any
SQL-level change.

**Recommendation:** Expose the consistency check logic behind a dedicated HTTP
endpoint or internal SQL function on the Materialize process, so testdrive only
needs a network client.

## Finding 2 — `Config` struct has poor locality

**Location:** `src/testdrive/src/action.rs:84-194`

`Config` is a single flat struct with ~100 fields covering Materialize pgwire,
Kafka, S3/AWS, Confluent Schema Registry, MySQL, DuckDB, SQL Server, and
Fivetran options. Adding a new external system requires touching this one struct
and `create_state`. The struct does not grow in size but every reader of any
config field imports every other system's config as context.

**Recommendation:** Group into per-system sub-structs (e.g., `KafkaConfig`,
`AwsConfig`) and embed them in `Config`. Existing field access patterns would
change minimally.

## Finding 3 — Built-in command dispatch is a string match with three-site edits

**Location:** `src/testdrive/src/action.rs:800-920`

Adding a new built-in command requires: (1) a new `mod` declaration in
`action.rs`, (2) a new `match` arm mapping the command name string, (3) the
implementation file itself. There is no registration mechanism; the match arm is
the sole link between the script parser and the implementation module.

**Risk:** Low — the pattern is consistent and the match is readable. But it
creates a friction cost when testdrive is extended for new external systems
(which has been happening frequently: DuckDB, SQL Server, Fivetran are recent
additions).
