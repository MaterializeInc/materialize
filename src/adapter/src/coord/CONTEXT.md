# adapter::coord

The `Coordinator` — Materialize's central state machine — its event loop, and all
sub-modules that partition its responsibilities. Approximately 28,576 LOC.

## File map

| File | What it owns |
|---|---|
| `coord.rs` | `Coordinator` struct (≈40 fields), `Message` enum, `ExecuteContext`, `serve`/`Config` entry points, `IdPool` |
| `command_handler.rs` | `handle_command` — dispatches external `Command` variants (session startup/teardown, Execute, Commit, webhook, system vars, auth, frontend-offload variants) |
| `message_handler.rs` | `handle_message` — dispatches internal `Message` variants (controller responses, timer ticks, staged-pipeline continuations, linearize reads, group commit) |
| `sequencer.rs` | `sequence_plan` top-level dispatch + RBAC check + shared utilities; see `sequencer/` |
| `sequencer/` | Per-statement sequencing; see [`sequencer/CONTEXT.md`](sequencer/CONTEXT.md) |
| `catalog_implications.rs` | `apply_catalog_implications` — converts `ParsedStateUpdate` diffs into downstream controller commands (create/drop dataflows, update read policies, manage VPC endpoints, cancel peeks) |
| `ddl.rs` | `catalog_transact` — wraps `catalog::transact` + `apply_catalog_implications` with telemetry and error propagation |
| `appends.rs` | Group-commit (user table writes), builtin-table append, write-lock machinery |
| `peek.rs` | `PeekDataflowPlan`, `FastPathPlan`, `implement_peek_plan` — ships peeks to compute or evaluates fast-path immediately |
| `read_policy.rs` | `ReadHolds` RAII, `initialize_read_policies`, `acquire_read_holds` — manages `since` on storage and compute collections |
| `timestamp_selection.rs` | `determine_timestamp`, `TimestampContext`, `TimestampDetermination`, `TimestampProvider` trait |
| `timeline.rs` | Timeline oracle management, `advance_timelines` |
| `ddl.rs` | (see above) |
| `catalog_serving.rs` | Serves read-only catalog snapshots to other modules; `auto_run_on_catalog_server`, `check_cluster_restrictions` |
| `introspection.rs` | Introspection-subscribe routing for built-in cluster logs |
| `cluster_scheduling.rs` | Periodic scheduling policy evaluation (`CheckSchedulingPolicies`) |
| `caught_up.rs` | 0dt read-only mode: checks whether all clusters/collections are caught up |
| `statement_logging.rs` | Statement execution tracking, sampling, writes to `mz_recent_activity_log` |
| `consistency.rs` | Debug-mode catalog consistency checks |
| `validity.rs` | `PlanValidity` — captures catalog preconditions for multi-stage pipelines |
| `indexes.rs` | Helpers for resolving and maintaining index metadata |
| `id_bundle.rs` | `CollectionIdBundle` — bundles storage + compute IDs for a set of collections |
| `in_memory_oracle.rs` | In-memory fallback timestamp oracle |
| `privatelink_status.rs` | PrivateLink VPC endpoint status routing |
| `sql.rs` | SQL parsing and planning utilities used by the coordinator |

## Key concepts

- **`Coordinator`** — a single-threaded `tokio` task that owns all mutable state (catalog, controllers, pending peeks, timeline oracles, write locks). External callers communicate via `Command`; internal async results arrive as `Message`.
- **`Message` enum** — the coordinator's main reactive dispatch surface; includes 9 `*StageReady` variants (one per staged statement type) plus timer ticks, controller responses, and housekeeping.
- **`Command` / `ExecuteContext`** — external `Command` is the client-facing request; `ExecuteContext` wraps it with a response channel and per-statement telemetry handles.
- **`apply_catalog_implications`** — the DDL pipeline's second phase: after `catalog::transact` commits durable diffs, this method translates them into controller mutations. The `CatalogImplication` state machine consolidates retract/add pairs into Altered transitions before dispatching.
- **`sequence_staged` driver** — a generic async loop that repeatedly calls `stage()` on a `Staged` impl until it returns `Immediate` (synchronous) or `Handle` (off-thread task re-queued via `internal_cmd_tx`).
- **Front-end offload commands** — `ExecuteSlowPathPeek`, `ExecuteSubscribe`, `ExecuteCopyTo`, `RegisterFrontendPeek` etc. are `Command` variants that bypass the coordinator's per-statement sequencing for slow-path query execution, reducing coordinator bottleneck.

## Architecture notes

- The coordinator's single-threaded event loop is the codebase's strongest invariant: all mutable state can be accessed without locks. Off-thread work is isolated by moving data into tasks and re-queuing results as `Message`.
- `catalog_implications.rs` (2,090 LOC) is the largest non-sequencer module. Its `apply_catalog_implications_inner` function dispatches a final per-object-type match; the `CatalogImplicationKind` state machine is a deepening device that consolidates retract+add pairs before dispatch.
- The `Message` enum grows by one variant per new staged statement type (verified: 9 `*StageReady` arms in `message_handler.rs`, all structurally identical). See `sequencer/ARCH_REVIEW.md` for details.

## Cross-references

- Caller: `mz-environmentd` calls `serve`; `mz-pgwire` uses `Client`/`SessionClient`.
- Below: `mz_controller::Controller` (compute + storage), `mz_catalog` (durable store).
- Sequencer: `sequencer/CONTEXT.md`, `sequencer/ARCH_REVIEW.md`, `sequencer/inner/ARCH_REVIEW.md`.
- Generated developer docs: `doc/developer/generated/adapter/coord/`.
