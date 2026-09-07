---
source: src/compute/src/compute_state/peek_budget.rs
revision: 82e054569f
---

# mz-compute::compute_state::peek_budget

Tracks how much cursor-position fuel a worker activation may spend walking index peeks before returning the worker to other work.

`InlineBudget` is the per-worker budget. `start_activation` discards whatever the previous activation left; `grant` returns the fuel one peek's slice may spend; `charge` deducts what a slice actually walked.

## Budget structure

`ActivationBudget` has two variants:

- `Unbounded` — every peek walks on the worker until it finishes or until its rows belong in the peek stash. This is what the kill switch (`ENABLE_INDEX_PEEK_OFFLOAD` off) restores.
- `Bounded { per_peek, remaining }` — a peek may spend `per_peek` positions before being offloaded, and all peeks together may spend `remaining` before the activation ends. A peek granted `None` (no remaining budget) must be passed over; stepping with no fuel suspends the scan without walking, offloading a peek that never had its inline turn.

`grant` returns the full `per_peek` budget or `None`; the aggregate can therefore overrun by one `per_peek`. This is intentional: partial grants would require the scan to handle mid-budget suspensions.

## Lazy arming

`InlineBudget` arms the first `ActivationBudget` on the first call to `grant` in each activation rather than at `start_activation`. Commands drain before the sweep that begins an activation, so a peek arriving on the command path is granted a slice before any activation begins. If the budget were armed at activation start, such a peek would receive whatever the previous activation left. Lazy arming is also what prevents the empty-snapshot problem: `handle_create_instance` applies the controller's configuration snapshot before replicas are created, so eagerly armed budgets armed before that snapshot is applied would see default values (offload disabled, budget unbounded) and hand the whole backlog a pass.

`InlineBudgetConfig` holds `ConfigValHandle` handles on the three dyncfg parameters (`ENABLE_INDEX_PEEK_OFFLOAD`, `INDEX_PEEK_INLINE_BUDGET`, `INDEX_PEEK_ACTIVATION_BUDGET`) so that activation-start reads avoid repeated lookups. Both `per_peek` and `remaining` are clamped to at least 1 so a zero configuration does not wedge: a per-peek budget of zero would suspend every scan before it walks anything, and an aggregate of zero would pass every peek over forever.
