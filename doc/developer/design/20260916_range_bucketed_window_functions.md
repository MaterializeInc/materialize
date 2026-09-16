# Range-bucketed window functions

- Associated: MaterializeInc/materialize#38851, MaterializeInc/materialize#38852,
  MaterializeInc/materialize#38855, MaterializeInc/materialize#38856,
  MaterializeInc/materialize#38888

## The Problem

`lag` and `lead` are rendered as a `Reduce::Basic` grouped by the `PARTITION BY`
key, with a single aggregate that receives the entire partition as a slice,
sorts it by the `ORDER BY` values, and returns one result per input row.
Differential re-invokes that closure with the full current contents of a
partition whenever *any* row in it changes.

So one changed row in a partition of `n` rows costs `O(n log n)`: a full
re-sort, a full re-walk, `n` output rows packed, and a consolidation of `2n`
owned `Row`s to cancel the old output against the new one.

The output of that work is tiny. `lag` and `lead` express a *relative*
relationship, not a positional one, so inserting a row in the middle of a
partition changes only the inserted row's result and the result of the row
`offset` positions after it:

```
before:  a   b       c   d        lag: a→ø  b→a  c→b  d→c
after:   a   b   m   c   d        lag: a→ø  b→a  m→b  c→m  d→c
                 ^new                             ^^^ only this changed
```

The true delta is `O(offset)`. The implementation spends `O(n log n)` to find
it. That ratio, roughly `partition size / offset`, is the opportunity.

This is specific to `lag` and `lead`. `row_number` and `rank` are genuinely
positional: inserting a row renumbers every row after it, so `O(n)` output
changes are unavoidable and no amount of restructuring helps.

## Success Criteria

- Per-update cost for `lag`/`lead` becomes a function of a bucket size that
  does not grow with the partition, rather than of the partition size.
- No regression in arrangement memory. This matters because the operators that
  motivate the work are already memory-bound, so trading memory for CPU makes
  their situation worse rather than better. Measured, the design below holds the
  same number of records but 26% to 31% more bytes, and the excess is per-row
  width rather than extra arrangements. See "Measured results".
- No change to results, with the exception discussed under
  "Ordering must be total" below.
- Opt-in, so that the existing path stays the default until the bucketing
  heuristic has been validated against real workloads.

## Out of Scope

- **`row_number`, `rank`, `dense_rank`.** Positional by definition, as above.
- **A monotonic fast path.** If a partition's input were known to be monotone
  in the `ORDER BY` key, `lag(x, k)` would only need the trailing `k` rows per
  partition, which is a much larger win (memory as well as CPU) but a separate
  mechanism. Note that `ReducePlan::create_from` already receives a `monotonic`
  flag and `ReductionType::Basic` ignores it.
- **Giving the reduce closure access to its previous output.** Differential's
  `reduce_core` passes `(key, input, previous_output, updates)`;
  `mz_reduce_abelian` uses the three-argument form and lets differential
  subtract the old output. Using the four-argument form would remove the
  consolidation of `2n` rows, but it is orthogonal: it applies per bucket
  exactly as it applies per partition, so the two compose.
- **Window frames** (`first_value`, `last_value`, window aggregates). The
  characterization below is specific to the fixed-offset lookback of
  `lag`/`lead`.

## Solution Proposal

Split each partition into contiguous **ranges** of the `ORDER BY` key, compute
`lag` within each range, and resolve the rows whose lookback crosses a range
boundary in a second, much smaller reduce.

Hierarchical reduction already exists for `min`/`max` and `TopK`
(`ReducePlan::Hierarchical` with `buckets = [256, 16]`), but it buckets by
`value.hashed() % buckets[layer]`, which destroys the order locality that `lag`
depends on. Bucketing by a monotone function of the `ORDER BY` key preserves it.

### The two levels

Level 0 is keyed by `(partition key, bucket)` and computes `lag` over the
bucket's rows alone. Level 1 is keyed by the partition key and runs the same
window function over a small subset of rows. Each level emits a result for a
disjoint set of rows, so the two outputs are combined with a union rather than
a join:

```
level 0:  reduce by (key, bucket)   emits rows resolved within their bucket
level 1:  reduce by (key)           emits rows whose lookback left their bucket
output:   level 0 ∪ level 1
```

Which rows each level owns is decided locally, from values level 0 already
computes.

**`IGNORE NULLS`.** Compute the local lag with a NULL default. The local result
is NULL exactly when the bucket holds fewer than `k` non-null values before the
row, so:

- *resolved* (level 0 emits): local `lag` IS NOT NULL.
- *target* (level 1 emits): local `lag` IS NULL.
- *summary* (level 1 needs as context): the bucket's last `k` non-nulls, which
  are the rows where the value is non-null and the local
  `lead(value, k) IGNORE NULLS` is NULL.

A user-supplied non-NULL `DEFAULT` breaks that test, since the default would
then be indistinguishable from a real value. Rather than strip the default and
reapply it after the union, level 0 gains one more constituent for such a call:
the same `lag` over the same value and offset but with a NULL default, used
only as the marker. The common case, a NULL default, needs no extra constituent
at all and reuses the call's own result.

**`RESPECT NULLS`.** Here a legitimate result can itself be NULL, so
resolution cannot be read off the value and is instead positional. Level 0
additionally computes `lag(1, k)` and `lead(1, k)` over the same window, both
with a NULL default:

- *resolved*: `lag(1, k)` IS NOT NULL. The constant is non-NULL for every row
  that has a `k`th predecessor and falls back to the NULL default otherwise, so
  this is a positional test wearing a value's clothing.
- *target*: `lag(1, k)` IS NULL.
- *summary*: `lead(1, k)` IS NULL, which holds exactly for the last `k` rows.

Two constants are used in preference to the more obvious `row_number() > k` and
`row_number() > count(*) - k` because neither of those would land in the same
operator. `row_number` is a *scalar* window function and `fuse_window_functions`
does not fuse those at all, and `count(*)` is an *aggregate* window function,
which fuses only with other aggregates. `lag` and `lead` are value window
functions, so with the same window, frame and `ignore_nulls` setting, which is
exactly the fusion key, all three calls fuse into one. Keeping the markers in
the same operator as the call they describe is the difference between adding
constituents to an existing reduce and adding whole reduces.

**Why level 1 has enough context.** For a target `t`, level 1 must contain
every row in `t`'s lookback window. Targets form a *prefix* of their bucket
under both rules (a row with `k` predecessors in the bucket is resolved, and so
is every row after it), and summaries form a suffix. So every row between a
target and the start of its bucket is itself a target, and the rows immediately
before the bucket are that bucket's predecessor's summaries. Under
`IGNORE NULLS` the same argument holds over the subsequence of non-nulls, which
is what `lead(value, k) IGNORE NULLS` selects.

**Fused windows.** Several `lag`s over one window are fused into a single
`FusedValueWindowFunc`, and one constituent can resolve locally while another
does not. Note that `ignore_nulls` is part of the fusion key, so a window
carrying both null modes is already two operators today and each is treated
separately. Routing individual columns to different levels would require a join,
so instead a row is a target if *any* constituent is unresolved, and level 1
computes every constituent for the rows it emits. Level 1's input is the union
of the per-constituent summaries. The prefix property above is what makes this
sound: the union of per-constituent prefixes is itself a prefix, so no row that
some constituent needs is missing.

**Every constituent must run the same way along the order**, and a window
mixing `lag` with `lead` therefore has to be left alone. The prefix property is
directional: for `lag` the unresolved rows are a bucket's prefix and the rows it
owes its neighbours are its suffix, and for `lead` it is the other way round.
Mixing them breaks the argument rather than just weakening it. A row can be a
target because a `lag` constituent ran off the front of its bucket, and level 1
then has to produce that row's `lead` as well, whose context is the rows
immediately after it. Those rows are neither targets nor summaries, so level 1
does not have them and reads past them into the next bucket, returning a value
from far too far away. This is not a subtle degradation: over randomized
partitions, mixed-direction windows produce a wrong answer in well over half of
trials, while single-direction windows are exact. Covering the mixed case would
mean giving every target a context window on both sides, which is a larger
change than it sounds and is not attempted here.

### Ordering must be total

The scheme is correct only if both levels agree on the order of rows whose
`ORDER BY` values are equal, because a target and a summary from the same
bucket must not be able to swap places between levels.

This is already the case. `order_aggregate_datums_with_rank_inner` passes the
payload datum, the `(OriginalRow, EncodedArgs)` record, as the tiebreaker to
`compare_columns`, so the comparison is total up to genuine row equality and
the two levels cannot disagree. Rows that compare fully equal are binary-equal
and therefore interchangeable.

Both levels must carry the same payload for this to hold, which the engine
implementation does by construction. It is worth stating explicitly because a
hand-written SQL version of this scheme does *not* get it for free: there the
two levels project different columns, so the implicit tiebreaker differs
between them and an explicit unique tiebreaker in the `ORDER BY` is required.

### Memory

Level 0's input and output arrangements are the same size as today's. Level 1's
are proportional to the number of buckets, which is small. Adding the bucket to
the key multiplies the number of distinct keys, which costs key storage but not
value storage.

The union of two collections is not itself an arrangement, so if a downstream
consumer needs an arranged form the plan pays for one more `n`-sized
arrangement. Measured, this does not materialize: the baseline already
maintains a separate index arrangement, so both plans hold about `3n` records.
See "Measured results".

## Measured results

A local `environmentd`, 400,000 rows in two partitions of 200,000, the
`ORDER BY` key spanning 40 bucket widths so each bucket holds about 5,000 rows.

**Per-update CPU.** Single-row inserts, each read back so the dataflow must
process it, with CPU taken from `mz_scheduling_elapsed` summed over the
operators of the dataflow under test. Two repetitions:

| | existing plan | bucketed plan |
|---|---|---|
| dataflow CPU per update | 1316 ms, 1315 ms | 243 ms, 235 ms |

Roughly 5.5x, reproducible across two different read styles.

NOTE: `mz_scheduling_elapsed` is itself a dataflow and lags behind the work it
reports. Reading it immediately after a batch of updates understates the delta,
badly and unevenly, and can return a zero delta outright. An earlier version of
this measurement did that and produced a much larger apparent speedup, which did
not reproduce. Let it settle for several seconds first.

Note also how much larger the baseline is than the evaluation path alone can
explain: a 200,000-row sort is tens of milliseconds, so most of the 1.3 seconds
is packing `n` output rows and letting differential consolidate `2n` owned
`Row`s to recover a delta of one or two. Bucketing shrinks that half as well,
which no amount of tuning inside the aggregate function would achieve.

**How it scales.** One partition, so every update recomputes all of it. Holding
rows-per-bucket at about 5,000 while the key span grows with the row count:

| rows | buckets | existing | bucketed | |
|---|---|---|---|---|
| 200,000 | 40 | 1424 ms | 240 ms | 5.9x |
| 800,000 | 160 | 7192 ms | 768 ms | 9.4x |

**Where it stops paying, and where it costs.** Holding the bucket count at 41 so
buckets shrink with the partition, and then confining a partition to a single
bucket so no split is possible:

| rows | buckets | existing | bucketed | | records |
|---|---|---|---|---|---|
| 100 | 41 | 3.72 ms | 3.71 ms | 1.00x | +39% |
| 1,000 | 41 | 7.90 ms | 4.32 ms | 1.83x | +7% |
| 10,000 | 41 | 49.5 ms | 11.5 ms | 4.31x | +0.6% |
| 100,000 | 41 | 569 ms | 139 ms | 4.08x | +0.05% |
| 10,000 | 1 | 50.7 ms | 62.3 ms | **0.81x** | +0.3% |
| 100,000 | 1 | 591 ms | 755 ms | **0.78x** | +0.02% |

Three things follow, and two of them argue for gating the rewrite rather than
applying it to everything eligible.

Break-even is around a thousand rows per partition. Below that there is nothing
to win, and the boundary level's arrangements, whose size is set by the bucket
count rather than by the row count, are a large relative overhead: 39% more
records at 100 rows.

**A partition confined to one bucket is 22% to 28% slower**, consistently at both
sizes tested. That is the rewrite paying for marker constituents, a second
reduce and a union while no split is possible, and it is reachable in practice:
with an hourly stride, any partition spanning less than an hour lands here. An
eligibility condition is needed, and a plain row-count threshold will not
express it, because what matters is how many buckets the partition's key span
actually covers.

At a fixed bucket count the speedup plateaus near 4x rather than approaching the
41x the bucket count would suggest. Bucket size grows with the partition, but
more importantly a per-update cost proportional to `n` survives bucketing
entirely: the bucketed cost still grew 3.2x between the 200,000 and 800,000 row
cases above even though bucket size was held constant. Whatever that residual is
(arrangement maintenance over an `n`-record trace is the obvious candidate) it
caps what this approach can deliver, and finding it is probably worth more than
tuning the bucket width.

**Arrangement cost.** Per-operator `mz_arrangement_sizes`, ten samples three
seconds apart, identical across every sample and both repetitions:

| | records | bytes |
|---|---|---|
| existing plan | 1,200,095 | 29.6 MiB |
| bucketed, `IGNORE NULLS` (one marker) | 1,200,375 | 37.3 MiB |
| bucketed, `RESPECT NULLS` (two markers) | 1,200,335 | 39.1 MiB |

**The record count is unchanged, so the union costs no extra arrangement.** Both
plans hold about `3n` records, because the baseline already maintains a separate
index arrangement alongside the reduce's input and output. The caveat above
about an indexed consumer paying for one more `n`-sized arrangement does not
materialize.

What costs 26% to 31% is row *width*, and it scales with the number of marker
constituents rather than with anything structural: one marker costs 7.7 MiB,
two cost 9.3 MiB. Each marker widens the value in level 0's input arrangement by
an args record and widens the result record in its output arrangement by a
field. That points the fix at the per-row encoding rather than at the two-level
shape, which is where MaterializeInc/materialize#38851 and
MaterializeInc/materialize#38852 already go: hoisting a constant offset and
default into the function, and sourcing the value argument from the original
row, would leave a marker costing close to nothing.

A narrower level 0 is also available in principle, since the markers' results
are read by the branch filters and then discarded, but the two branches want
different projections of the same arrangement, so one narrow arrangement cannot
serve both.

Two traps worth recording. `mz_internal.mz_object_arrangement_sizes.size` is
rounded to the nearest 10 MiB, which is the same size as this effect, so it
cannot measure it; the first attempt with it produced a meaningless 30-vs-40
reading. And timing client round trips finds nothing at all, because ~30ms of
per-round-trip cost buries even a 2-second operator recomputation.

## Alternatives

**Hash bucketing**, as `Reduce::Hierarchical` does today. Destroys the order
locality `lag` depends on, so a bucket's rows are not contiguous in the sort
order and no local computation is meaningful.

**Overlapping buckets.** If each bucket contained its own rows plus the last
`k` rows of the preceding bucket, every row's lookback would be inside its own
bucket and no second level would be needed. This is more elegant but not
reachable: "the last `k` rows of the preceding bucket" is a rank-based notion,
not a value-range predicate, so identifying those rows already requires the
global sort the scheme is trying to avoid.

**Carry injection, avoiding the extra arrangement.** Instead of a second level
that emits results, compute each bucket's incoming context with a `TopK` over
`(key, bucket)` (which is itself hierarchical and incremental), run a small
window reduce over those summaries to get each bucket's carry, and inject the
carry into level 0's input as tagged synthetic rows. Level 0 then resolves
everything locally and there is a single output arrangement. This is strictly
better on memory and expressible with existing MIR nodes (`TopK`, `Reduce`,
`Union`), at the cost of a third operator and a tag column. It is the better
end state and the worse first step.

**Reformulating as a self-join.** Joining the relation to itself on
`rank = rank - 1` inherits `row_number`'s genuine non-incrementality. Joining
each row to the row with the greatest smaller `ORDER BY` key needs a `LIMIT 1`
per row rather than per group, which `TopK` cannot express. The predecessor
relation is the hard part and both framings assume it.

## Open Questions

**How is the bucket width chosen?** This is the one genuinely unresolved
question, and it is exactly why the existing hierarchical path uses hashes: a
monotone bucket function needs some knowledge of the key distribution, whereas
a hash needs none.

Measurements over a simulated 8000-row partition put the peak near `sqrt(n)`
rows per bucket, as expected, but more usefully show a wide tolerant band. At
200 to 400 rows per bucket, every combination of one or four fused constituents
and null densities of 0, 30 and 70% reduces modelled per-update work by at
least 21x, with a peak of 54x for a single constituent over dense data. Smaller
buckets are where the choice starts to matter: at 90 rows per bucket the same
sweep ranges from 12x to 54x, and at 50 rows from 6.7x to 35x, because level 1
grows as buckets shrink and eventually dominates.

The practical reading is that the width only has to be large enough, not
correct, and that erring high is much safer than erring low. A fixed width
applied to the high bits of an integer, date, or timestamp key is a plausible
default. Note that the existing hierarchical path already builds its bucket
ladder in powers of 16 from a fan-in of 16, so a comparable fixed choice here
would not be a new kind of magic constant.

Options, roughly in increasing ambition: a `dyncfg`-supplied constant; a
per-object hint in the spirit of `EXPECTED GROUP SIZE` (which today reaches only
`bucketing_of_expected_group_size` and so does nothing at all for window
functions); or statistics-driven selection.

**Eligibility.** Besides the single-direction requirement above, the offset
argument of `lag`/`lead` is an arbitrary expression evaluated per row, so the
lookback distance is not statically known in general. Hoisting constant offsets into the function
(MaterializeInc/materialize#38851) makes the common case statically visible,
and the optimization should require it. The `ORDER BY` also needs a leading
column of a type with a natural monotone coarsening.

**`IGNORE NULLS` worst case.** With `IGNORE NULLS` the lookback is unbounded,
so a bucket consisting entirely of nulls contributes all of its rows to level 1
and the scheme degrades toward the current behaviour. With four constituents at
200 rows per bucket, level 1 measured 1.9% of the partition at 30% nulls and
4.5% at 70%, so the degradation is graceful, but it is data-dependent rather
than bounded.

## Validation

The characterization above was checked two ways before any engine code was
written.

A model implementation was compared against a naive `lag` over randomized
partitions: 6000 trials varying partition size, offset, bucket width, null
density, and the number of fused constituents, in both null modes, with and
without heavy `ORDER BY` ties. All agree once the order is total, and the
earlier version that tested resolution by value rather than by position in
`RESPECT NULLS` mode is exactly what the tie and null-mode trials caught.

Worth recording how the mixed-direction restriction was found, because it says
something about where the risk in this design lives. The model covered several
fused constituents but gave them all the same direction, so it agreed with the
reference and the scheme looked sound. The repository's existing
`window_funcs.slt` then produced a wrong answer for a query selecting both
`lag(a)` and `lead(a)` over one window. Every boundary condition in this design
is directional, and a validation harness that varies everything except
direction will report success.

The scheme is also expressed directly in SQL, as two levels of views over a
table with ties and nulls, asserting that the union of the two levels matches a
plain `lag` row for row in both directions. That form is what an implementation
should be checked against, and it doubles as a workaround available to users
today, subject to the explicit-tiebreaker caveat above.
