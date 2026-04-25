---
source: src/adapter/src/coord/sequencer.rs
revision: 618f264dfb
---

# adapter::coord::sequencer

Top-level sequencer module: implements `Coordinator::sequence_plan`, which matches on each `Plan` variant and dispatches to the appropriate `sequence_*` method.
The module also contains shared utilities used across statement types: `statistics_oracle` (wraps the transform statistics oracle), `eval_copy_to_uri` (validates the COPY TO URI, accepting `s3://` and `gs://` schemes), and `return_if_err!` macro (short-circuits on error, sending a response to the client).
The `inner` sub-module holds most per-statement implementations; this file ties them together and handles generic concerns like RBAC checks and transaction validation.
