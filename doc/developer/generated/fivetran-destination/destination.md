---
source: src/fivetran-destination/src/destination.rs
revision: 849327076c
---

# mz-fivetran-destination::destination

`MaterializeDestination` implements the `DestinationConnector` tonic trait, handling all Fivetran Destination gRPC RPCs.

Three Fivetran system columns are recognized: `_fivetran_deleted` (soft-delete flag), `_fivetran_synced` (last-modified timestamp), `_fivetran_id` (synthesized primary key).

Each RPC that modifies data wraps its handler in `with_retry_and_logging`, which retries `OpError`s whose `can_retry()` returns `true` (connection failures, transient duplicate-object errors) with exponential backoff, and logs all attempts.

Submodules `config`, `ddl`, and `dml` contain the actual handler logic.
