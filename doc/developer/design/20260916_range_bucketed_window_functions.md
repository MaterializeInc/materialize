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
  their situation worse rather than better.
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

A user-supplied non-NULL `DEFAULT` is applied after the union, so that level 0
can use NULL as its "unresolved" marker without ambiguity.

**`RESPECT NULLS`.** Here a legitimate result can itself be NULL, so
resolution cannot be read off the value and is instead positional. Level 0
additionally computes `row_number()` and `lead(1, k)` over the same window:

- *resolved*: `row_number > k`.
- *target*: `row_number <= k`.
- *summary*: `lead(1, k)` IS NULL, which holds exactly for the last `k` rows of
  the bucket, since the constant is non-NULL for every row that has a `k`th
  successor and falls back to the NULL default otherwise.

`lead(1, k)` is used in preference to `row_number > count(*) - k` because
`count(*)` is an aggregate window function, and `fuse_window_functions` keys on
the distinction between value and aggregate calls, so it would land in a
separate operator. The three value calls above share a window and an
`ignore_nulls` setting, which is exactly the fusion key, so they land in one.

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
arrangement. Whether that is acceptable, or whether the carry-injection variant
below should be preferred, is an open question.

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

**Eligibility.** The offset argument of `lag`/`lead` is an arbitrary
expression evaluated per row, so the lookback distance is not statically known
in general. Hoisting constant offsets into the function
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

The scheme is also expressed directly in SQL, as two levels of views over a
table with ties and nulls, asserting that the union of the two levels matches a
plain `lag` row for row in both directions. That form is what an implementation
should be checked against, and it doubles as a workaround available to users
today, subject to the explicit-tiebreaker caveat above.
