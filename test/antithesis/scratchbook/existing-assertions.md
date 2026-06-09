# Existing Antithesis SDK Assertions

## Summary

**No Antithesis SDK assertions exist in the Materialize source code.**

A comprehensive search of the Rust codebase at `materialize/src/` found:

- No `use antithesis` import statements
- No Cargo.toml dependencies on any antithesis crate
- No assertion macros: `assert_always!`, `assert_sometimes!`, `assert_reachable!`, `assert_unreachable!`
- No antithesis function calls in the Python test code within the materialize repository

## Existing Antithesis Integration (Customer Level)

Antithesis integration exists at the **customer-repo level** (outside the materialize source), using the legacy experiment-script approach:

### Experiment Scripts (`guest/opt/antithesis/experiment/`)

- **`materialize.py`**: Docker Compose-based experiment. Uses `antithesis.start_customer_containers()`, `antithesis.start_fault_injector()`, `antithesis.run_process()`, `antithesis.fuzz_msg()`, `antithesis.end_test()`. Orchestrates testdrive workloads with network chaos (latency, packet loss, partitions).
- **`testdrive.py`**: K8s-based variant. Sets up k3s cluster with minio, redpanda, postgres, environmentd. Runs testdrive via kubectl.
- **`materialize-k8s.sh`**: Bash setup for K8s resources.

### Docker Compose Topology (`guest/opt/materialize/docker-compose.yml`)

Uses custom Antithesis-instrumented images:
- `antithesis-cp-combined` (Kafka + Schema Registry)
- `antithesis-materialized` (Materialize)
- `antithesis-testdrive` (Test workload)

### K8s Manifests (`guest/opt/materialize/k8s/antithesis/`)

Full Kubernetes topology: environmentd StatefulSet, postgres StatefulSet, redpanda Deployment, testdrive Pod, with PVs and services.

## Implications for New Work

All property assertions will need to be added fresh. The existing integration provides a starting point for topology but uses an older approach (experiment scripts, custom instrumented images). The new approach should leverage mzcompose for compose generation and add Antithesis SDK assertions either in the workload client or (for deeper coverage) in the Materialize Rust source.

## Storage/Kafka/UPSERT Path — Candidate Instrumentation Sites

Added 2026-05-11 during Kafka-source property discovery. These are existing `panic!`/`assert!`/`unreachable!` sites in the storage code that are direct candidates for being wrapped with the Antithesis SDK so that violations surface as reportable property failures rather than process aborts. Confirmed by grepping the source at commit `007c7af9d9970fb2030c7212368b232e0fbc363e`.

### `src/storage/src/source/kafka.rs`

- `:158` — `expect("positive pid")`
- `:265` — `expect("all source exports must be present in source resume uppers")`
- `:276` — `panic!("unexpected source export details: {:?}", details)`
- `:282` — `expect("statistics have been initialized")`
- `:345` — `expect("restored kafka offsets must fit into i64")`
- `:606, :853, :855, :891, :894, :897, :903, :907, :997` — various `expect()` and `assert!()` on reader state
- `:1142-1147` — `assert!(self.last_offsets[output_index].contains_key(&partition))`
- `:1193-1197` — `panic!("got negative offset (...) from otherwise non-error'd kafka message")`
- `:1209` — `expect("kafka sources always have upstream_time")`
- `:1457` — `assert!(…)` on payload structure

### `src/storage/src/source/reclock.rs` and `reclock/compat.rs`

- `reclock.rs:124` — `assert!(!new_into_upper.less_equal(&binding_ts))`
- `reclock.rs:321` — `assert!(prev < RB::before(pid))`
- `reclock/compat.rs:144` — `assert!(…)` on persist handle state
- `reclock/compat.rs:306` — `panic!("compare_and_append failed: {invalid_use}")`

### `src/storage/src/upsert.rs`

- `:541` — `assert!(diff.is_positive(), "invalid upsert input")`
- `:636` — `panic!("key missing from commands_state")`
- `:1031` — `unreachable!("pending future never returns")`

### `src/storage/src/upsert_continual_feedback.rs`

- `:626` — `assert!(diff.is_positive(), "invalid upsert input")`
- `:800` — `panic!("key missing from commands_state")`

### `src/storage/src/upsert_continual_feedback_v2.rs`

- `:315` — `assert!(diff.is_positive(), "invalid upsert input")`
- `:483` — `unreachable!()` on `(None, None)` from joined prior/new state

### `src/storage/src/upsert/types.rs` — `StateValue` and `ensure_decoded`

- `:297, :369, :403, :416, :430, :440` — six `panic!("called \`<accessor>\` without calling \`ensure_decoded\`")` sites (`into_decoded`, `into_provisional_value`, `into_provisional_tombstone`, `provisional_order`, `provisional_value_ref`, `into_finalized_value`)
- `:580` — `panic!("\`merge_update_state\` called with non-consolidating state")`
- `:621` — `assert_eq!(checksum_sum.0, seahash::hash(value) as i64, …)` inside `ensure_decoded` (diff_sum == 1)
- `:632, :637, :642` — three checks for `diff_sum == 0` (`len_sum`, `checksum_sum`, all-zero `value_xor`)
- `:672` — `panic!("invalid upsert state: non 0/1 diff_sum: …")`
- `:1062` — `panic!("attempted completion of already completed upsert snapshot")`

Per the property catalog, each of these gets a *distinct, specific* Antithesis assertion message so a fired assertion names exactly the site reached. No site shares a message with another. See `properties/upsert-no-internal-panic.md`, `properties/upsert-state-consolidation-wellformed.md`, `properties/upsert-ensure-decoded-called-before-access.md`, and `properties/kafka-source-no-internal-panic.md` for the per-site rename table.
