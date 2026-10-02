---
source: src/timely-util/src/funded_spine/spine_fueled.rs
revision: bb5c454adc
---

# timely-util::funded_spine::spine_fueled

`Spine<B>`: an append-only collection of update batches that merges immutable batch layers with amortized effort, with optional exertion funded by inserted updates.

This is a copy of differential-dataflow's `spine_fueled.rs` (0.25.1) with crate-internal paths rewritten and two funding fields added. See the `funded_spine` module documentation for the funding model.

## Data structure

The spine is a list of `MergeState` layers, each of which is:

- **`Vacant`** — empty.
- **`Single(Option<B>)`** — a single batch. `None` represents a structurally empty batch used for bookkeeping.
- **`Double(MergeVariant<B>)`** — two batches currently merging, either `InProgress` or `Complete`.

Layer `i` holds at most `2^i` elements. The invariant maintained is that for any in-progress merge at level `k`, there are fewer than `2^k` records at lower levels, ensuring the merge completes before lower levels can promote into it.

## Key operations

- **`insert`** — receives a new batch, adds `CONSOLIDATION_CREDIT_PER_UPDATE * effort` credit per record and `CONSOLIDATION_GRANTS_PER_PROGRESS` allowances to the bank (capped at `MAX_BANKED_PROGRESS_GRANTS = 64`), then calls `consider_merges` to promote pending batches into the merging layers.
- **`exert`** — applies optional maintenance effort. A funded request either applies `effort` units of fuel to in-progress merges via `apply_fuel`, or introduces a virtual batch at the corresponding level via `introduce_batch`. A request is declined and the operator is not rescheduled when no funding is available and no merge is already in progress.
- **`introduce_batch`** — applies fuel to in-progress merges, rolls up lower layers, inserts the batch at the appropriate level, then tidies the largest layers.
- **`apply_fuel`** — distributes fuel independently to each in-progress merge; a completed merge is immediately promoted to the next level.
- **`tidy_layers`** — draws down the largest layer to a smaller level when its record count warrants it, provided the invariant is not violated.

## Funding constants

| Constant | Value | Meaning |
|---|---|---|
| `CONSOLIDATION_CREDIT_PER_UPDATE` | 8 | Credit per inserted record (times `effort` multiplier) |
| `CONSOLIDATION_GRANTS_PER_PROGRESS` | 8 | Allowances added to the bank per inserted batch |
| `MAX_BANKED_PROGRESS_GRANTS` | 64 | Maximum banked allowances |

## `Spine` construction

`Spine::with_effort(effort, operator, logger, activator)` — constructs a spine with the given effort multiplier. An effort of zero is treated as one.
