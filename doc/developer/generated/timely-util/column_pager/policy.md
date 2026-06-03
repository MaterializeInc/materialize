---
source: src/timely-util/src/column_pager/policy.rs
revision: bfa6499c3b
---

# timely-util::column_pager::policy

Concrete `PagingPolicy` implementations for the column pager.

## `TieredPolicy`

A single-pool byte budget for resident columns backed by a process-wide `AtomicUsize`.

Each call to `PagingPolicy::decide` tries to reserve `len_bytes` from the pool via a compare-exchange loop (`try_consume`). If the reservation succeeds the policy returns `PageDecision::Skip` (column stays resident); otherwise it returns `PageDecision::Page` with the currently configured `backend` and `codec`, read atomically at call time. `PagingPolicy::record` credits bytes back to the pool on `PageEvent::ResidentReleased`; all other event variants are ignored.

### Why a single global pool

Resident columns can move between Timely workers freely, so accounting cannot be thread-local. A cross-thread drop on a thread-local pool would silently drift both budgets. The single-atomic design credits the same pool regardless of which thread drops the column. A future thread-aware policy could use either a `SendColumn` wrapper pinning the column to its originating thread, or an explicit cross-thread credit channel keyed by `std::thread::ThreadId`.

### Fields

| Field | Type | Purpose |
|---|---|---|
| `budget` | `AtomicUsize` | Remaining bytes available for resident columns; drained on `decide` (Skip), refilled on `record(ResidentReleased)` |
| `configured` | `AtomicUsize` | Last-configured total; `reconfigure` adjusts `budget` by the delta against this value so in-flight `ResidentTicket`s stay coherent after an operator-driven tune |
| `backend` | `AtomicU8` | Backend selection for `PageDecision::Page` outcomes, encoded as a `u8` |
| `codec` | `AtomicU8` | Codec selection for `PageDecision::Page` outcomes, encoded as a `u8` |

### `reconfigure`

Adjusts the policy in place. The budget moves by `new_total - prev_total` so in-flight `ResidentTicket`s that still credit this same atomic when they drop remain coherent with the resized pool. Backend and codec selection take effect on the next `decide` call. Shrinking the configured total below the in-flight resident set saturates the available budget at zero; subsequent `decide` calls page out until releases bring the pool back above zero.

### `configured_total`

Returns the most-recently-configured total budget. Useful for tests.

### `try_consume`

A private helper that atomically subtracts `want` from an `AtomicUsize` if at least `want` is available, using a compare-exchange-weak loop. Returns `true` on success and `false` if the current value is less than `want`.
