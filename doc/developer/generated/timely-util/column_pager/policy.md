---
source: src/timely-util/src/column_pager/policy.rs
revision: cc7f2656e3
---

# timely-util::column_pager::policy

Concrete `PagingPolicy` implementations for the column pager.

## `TieredPolicy`

A single-pool byte budget for resident columns backed by a process-wide `AtomicUsize`.

Each call to `PagingPolicy::decide` tries to reserve `len_bytes` from the pool via a compare-exchange loop (`try_consume`). If the reservation succeeds the policy returns `PageDecision::Skip` (column stays resident); otherwise it returns `PageDecision::Page` with the currently configured `backend` and `codec`. `PagingPolicy::record` credits bytes back to the pool on `PageEvent::ResidentReleased`; all other event variants are ignored.

### Why a single global pool

Resident columns can move between Timely workers freely, so accounting cannot be thread-local. A cross-thread drop on a thread-local pool would silently drift both budgets. The single-atomic design credits the same pool regardless of which thread drops the column. A future thread-aware policy could use either a `SendColumn` wrapper pinning the column to its originating thread, or an explicit cross-thread credit channel keyed by `std::thread::ThreadId`.

### Fields

| Field | Type | Purpose |
|---|---|---|
| `budget` | `AtomicUsize` | Remaining bytes available for resident columns |
| `backend` | `Backend` | Backend selection for `PageDecision::Page` outcomes |
| `codec` | `Option<Codec>` | Codec selection for `PageDecision::Page` outcomes |

### `try_consume`

A private helper that atomically subtracts `want` from an `AtomicUsize` if at least `want` is available, using a compare-exchange-weak loop. Returns `true` on success and `false` if the current value is less than `want`.
