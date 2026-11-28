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
- A way out for partitions the split cannot help, which is a separate criterion
  from the one above and the harder one to meet. See "Eligibility".
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

The union of two collections is not itself an arrangement, so a downstream
consumer that needs an arranged form might be expected to pay for one more
`n`-sized arrangement. It does not: an indexed consumer already maintains an
arrangement of the reduce's output today, so the union's output takes its place
rather than adding to it, and the row count held across the plan is unchanged.

What the split does cost is row width. Each marker constituent widens the value
in level 0's input arrangement by an args record and its result record by a
field, so the same rows are stored slightly larger. That places the cost in the
per-row encoding, which is where
MaterializeInc/materialize#38851 and MaterializeInc/materialize#38852 already
go: with a constant offset and default hoisted into the function, and the value
argument sourced from the original row, a marker costs close to nothing.

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

**How is the bucket width chosen?** A `WINDOW BUCKET WIDTH` query hint, taking
a duration for a temporal key and a count of the key's own units for an integer
one, because the two share no unit: `'1 day'` means nothing to a version
counter, and `4096` means nothing in particular to a timestamp. A hint whose
form does not match the key is ignored rather than raising, since falling back
beats failing a query over a hint. Absent the hint the width falls back to a
constant per key type.

The hint is deliberately the weakest instrument that solves the problem. It sets
a divisor inside one expression, so the plan's shape, its operator count and its
results are identical whatever value it takes; an unhelpful value costs
performance and nothing else. That is less than the existing group size hints
do, since `AGGREGATE INPUT GROUP SIZE` feeds `bucketing_of_expected_group_size`
and so decides how many levels the hierarchical reduce renders. A hint that
switched this optimization on or off would be a stronger thing again, and is not
what this is.

Naming follows the three hints that replaced `EXPECTED GROUP SIZE`, which were
introduced because a generic name did not say which operation it tuned.

The name says what the value is: the width of one bucket, not the extent of the
data. The width has to be matched to the key's density, and the failure is
two-sided.
Too wide and the partition falls into one bucket, where the split cannot help
and its overhead is pure loss. Too narrow and the boundary level approaches the
whole partition, so the work is done twice. Neither is expressible as a fixed
constant, because what a good width is depends on the units someone chose for
their key: seconds and milliseconds since the epoch differ by three orders of
magnitude, and a version counter differs from both. The fallback constants are
therefore a convenience for keys shaped like epoch seconds or hourly timestamps,
not a general answer, and anything else should carry the hint.

Deriving the width from statistics remains the better long-term answer. Note
that the leading `ORDER BY` key's distribution is exactly what would be needed,
and is not something the planner has today.

**Eligibility.** A partition whose key span falls inside a single bucket width
cannot be split at all, so the rewrite buys nothing and still pays for its
marker constituents, its second reduce and its union. Such a partition is
materially slower than it is today.

The condition is not a property of the plan: what decides it is how many buckets
the partition's key span covers, which depends on the data, so neither a
row-count threshold nor anything else available at planning time can express it.
Nor can type-based exclusion, since what matters is a key's range rather than
its type, and the clearest example of the problem is an `int4` version counter
whose values never approach what the type allows. The mitigation is the width
hint above, which lets such a window be bucketed usefully rather than not at
all, and the feature flag remains the blunt instrument for turning the rewrite
off entirely.

Besides that and the single-direction requirement above, the offset argument of
`lag`/`lead` is an arbitrary expression evaluated per row, so the lookback
distance is not statically known in general. Hoisting constant offsets into the
function (MaterializeInc/materialize#38851) makes the common case statically
visible, and the optimization should require it. The `ORDER BY` also needs a
leading column of a type with a natural monotone coarsening.

**`IGNORE NULLS` worst case.** With `IGNORE NULLS` the lookback is unbounded,
so a bucket consisting entirely of nulls contributes all of its rows to level 1
and the scheme degrades toward the current behaviour. The degradation is
graceful rather than sudden, since the boundary level grows with null density
rather than jumping, but it is data-dependent rather than bounded.

## Validation

The characterization above is checked two ways, both independent of the
implementation.

A model implementation is compared against a naive `lag` over randomized
partitions, varying partition size, offset, bucket width, null density, the
number of fused constituents and their directions, in both null modes, with and
without heavy `ORDER BY` ties. Single-direction windows agree exactly;
mixed-direction windows disagree in the majority of trials, which is the
evidence behind the restriction above.

The scheme is also expressed directly in SQL, as two levels of views over a
table with ties and nulls, asserting that the union of the two levels matches a
plain `lag` row for row in both directions. That form is what an implementation
should be checked against, and it doubles as a workaround available to users
today, subject to the explicit-tiebreaker caveat above.

Both are worth keeping in mind when extending this: every boundary condition
here is directional, so a harness that varies everything except direction will
report success on a design that is wrong.
