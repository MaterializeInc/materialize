---
source: src/adapter/src/util.rs
revision: e66b095a0e
---

# adapter::util

Provides miscellaneous coordinator utilities.
`ClientTransmitter<T>` and the `Transmittable` trait implement the one-shot response channel from the coordinator to a waiting client, with optional soft-asserts that only permitted `ExecuteResponse` variants are sent; `CompletedClientTransmitter` pairs a completed execute context with a transaction-end response.
`ResultExt` extends `Result` with `unwrap_or_terminate` / `maybe_terminate`, which call `exit!(0)` on errors that implement `ShouldTerminateGracefully` (e.g. catalog deploy-generation fencing) instead of panicking.
Additional utilities include `index_sql` (generates a canonical `CREATE INDEX` SQL string), `describe` (invokes the SQL planner's describe path), `viewable_variables` (merges session and catalog system variables for SHOW), and `sort_topological` (Kahn's algorithm for dependency-ordered iteration).
