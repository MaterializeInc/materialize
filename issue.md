# adapter: subscribe to catalog changes from other adapter processes

Scalability, use-case isolation, and zero-downtime upgrades v2 (see [design doc](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20251219_zero_downtime_upgrades_physical_isolation_high_availability.md)) all require multiple `environmentd`/adapter processes to collaborate on shared catalog state. This issue is about enabling an adapter process to **subscribe to catalog changes written by _other_ adapter processes** and apply their implications (update in-memory state, update builtin tables, send controller commands).

This is a follow-up to #8488, which restructures the single-process code so that all implications are derived from catalog changes rather than being computed inline during transaction execution. Once that refactor is complete, the same "derive implications from changes" logic can be driven by a subscription to the durable catalog, not just by self-initiated transactions.

## What this requires beyond #8488

**#8488** is about the code architecture within a single process: restructure the coordinator so that implications are derived _after_ the durable catalog is updated, from the catalog changes themselves. The process still knows what it wrote — it derives implications from its own transaction's changes.

**This issue** is about handling changes that arrive from _other_ writers:

- **Use the subscription mechanism in production.** [`Catalog::sync_to_current_updates`](https://github.com/MaterializeInc/materialize/blob/main/src/adapter/src/catalog.rs) already exists as a mechanism for subscribing to catalog changes and applying them to in-memory state. However, it is currently gated behind `#[cfg(test)]` and not used in production (see `TODO(jkosh44)` on that method). This issue includes making that code path production-ready and integrating it into the coordinator's main loop.

- **Handle externally-initiated changes.** When changes arrive via subscription that were written by a _different_ adapter process, the local process must apply them through the same implication-derivation logic that #8488 establishes. This is distinct from the #8488 case where the process knows what it wrote and derives implications from its own transaction output.

- **Distinguish self-initiated vs. external changes.** The process needs to tell apart changes it wrote itself (which it has already processed) from changes written by other processes (which it needs to react to). This avoids double-applying implications.

## Relationship to the design doc

This is the "read side" of the capability described in the [zero-downtime upgrades design doc](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20251219_zero_downtime_upgrades_physical_isolation_high_availability.md), specifically the requirement that "multiple instances of `environmentd` need to be able to subscribe to catalog changes and collaborate in writing down changes to the catalog" (section: Work Required, item 1).

## Not in scope

- The "write side" — multiple processes collaborating on _writing_ to the catalog — is separate.
- Builtin tables/sources that are not derived from catalog state (e.g., `mz_sessions`, storage-usage data) are tracked separately.

## Depends on

- #8488 (derive implications from catalog changes within a single process)
