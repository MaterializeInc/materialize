---
source: src/adapter/src/coord/sequencer.rs
revision: a702b8be70
---

# adapter::coord::sequencer

Top-level sequencer module: implements `Coordinator::sequence_plan`, which matches on each `Plan` variant and dispatches to the appropriate `sequence_*` method.
The module also contains shared utilities used across statement types: `statistics_oracle` (builds a cardinality-estimate oracle for query optimization), `eval_copy_to_uri` (validates the COPY TO URI, accepting `s3://` and `gs://` schemes), `check_log_reads` (validates introspection-source reads and enforces replica targeting on multi-replica clusters), `emit_optimizer_notices` (forwards optimizer notices to the session), `explain_pushdown_future_inner` (computes filter-pushdown statistics for EXPLAIN FILTER PUSHDOWN), `explain_plan_inner` (generates EXPLAIN PLAN output), and the `return_if_err!` macro (short-circuits on error, sending a response to the client).
The `inner` sub-module holds most per-statement implementations; this file ties them together and handles generic concerns like RBAC checks and transaction validation.
