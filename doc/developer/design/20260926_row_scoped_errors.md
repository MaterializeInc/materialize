# Row-scoped errors

* Associated: builds on cell-scoped errors, [MaterializeInc/materialize#39172](https://github.com/MaterializeInc/materialize/pull/39172) and `20260926_cell_scoped_errors.md`.
* Prior work: the two-diff model of [#36614](https://github.com/MaterializeInc/materialize/pull/36614) and [#36700](https://github.com/MaterializeInc/materialize/pull/36700).

## The problem

Cell-scoped errors keep an error in a map expression in its column, but every other error still lands in the error collection.
Errors in predicates, join equalities, aggregates, top-k, and table-function arguments carry no row, and each one fails every read of the collection.
A standing-query setup multiplexes many parameterized queries through one dataflow: a table of parameters, a join against the data, and one `SUBSCRIBE`.
One query whose predicate errors on one row fails the subscribe for every query, and the error cannot be retracted.
This design attributes such errors to rows, so that a consumer sees which rows are affected and everything else keeps working.

## Success criteria

* An error in a predicate or join closure is attributed to the rows whose existence it affects, and retracts with them.
* Rewrites the optimizer applies today stay correct, and results do not depend on the plan beyond one documented case.
* Errors that have no row, such as key evaluation and decode errors, keep today's collection scope.
* Without opting in, peeks and subscribes behave as today.

## Out of scope

* Redacting error payloads. Payloads contain values, see the access-control section.
* Persisting tagged rows in materialized views before the staging step that adds a format for it.

## Representation

### Options

Three encodings mark a row as an error.

* **A pair diff `(ok, err)`.**
  The diff ring `Z[e]/(e² = e)`, with product `(a + be)(c + de) = ac + (ad + bc + bd)e`, is a sound commutative ring and the product of the #36614 Lean model.
  It carries counts, not messages, so a predicate error such as `WHERE 1/x > 0`, which leaves no error cell, still needs its message in the row.
  It widens every diff in the system, and changing `mz_repr::Diff` touches about 1,800 occurrences in 135 files.
* **One bit of the `i64` diff.**
  Raw bits carry into the count, an xor bit cancels two copies of an error row, and an or bit has no inverse, so errors could not be retracted.
  No variant says "two ok copies and one error copy", so the flag belongs in the data.
* **A tag in the row.**
  Moving the tag from the diff into the data is an isomorphism, `Z[D][M] ≅ Z[D × M]` with `M = ({ok, err}, or)`, so a row tag with an `i64` diff has the algebra of the pair diff.
  It also has room for the message, and it changes neither the diff type nor any durable format.

This design uses a tag in the row.

### The row-level error

A tainted row stores its error in a header ahead of its datums, `RowRef::row_error`.
The header is not a datum: `RowRef::iter` skips it, so code that reads datums by position is unaffected, and MIR does not see it.
The diff stays `i64`, and a tainted row consolidates, retracts, and arranges like any row, with the header part of its bytes and therefore of its identity.

A hidden trailing column was the alternative, and it is fragile in two places.
Readers that decode only a prefix of a row, such as demand-pruned MFP inputs and reduce, would drop it without notice.
Joins concatenate the values of their inputs, so a trailing column of one input would land in the middle of the output.
A header is found by reading one byte, whatever prefix a reader decodes.

The header is lost wherever a row's datums are repacked into a new row, so repacking code carries it over explicitly: MFP output, arrangement values, and join key preparation.
Code that decodes a row for MFP evaluation appends the row-level error as an error datum after the decoded columns, where `SafeMfpPlan::evaluate_inner_scoped` expects it at index `input_arity`.
Join closures append the greater row-level error of the two matched sides instead.
Durable encodings refuse rows with a row-level error, and dictionary compression refuses to encode them.

The payload is one `EvalError`.
When two errors meet on one row, the payload is the `max` of the two under `EvalError`'s derived order, the combiner scalar `AND` already uses.
`max` is commutative, associative, and idempotent, so the payload does not depend on evaluation order, which replicas and retractions both need.
Error cells stay in their columns next to the tag.

## Semantics

### Principle

A tainted row means that its existence depends on an error that could not be evaluated.
Every rule below follows from one principle:

> An output row is tainted if some resolution of some error changes it.
> An output row is absent if it is absent under every resolution.

A resolution picks, for each error, a value or "the row does not exist".
The principle makes the algebra exact rather than a convention.
A tainted row that matches nothing produces nothing, because no resolution produces output.
Two uncertain copies are two uncertain copies.

### Predicates

A predicate maps each row to a multiplier: `TRUE` keeps it, `ERROR` keeps it and tags it, `FALSE` and `NULL` drop it.
Tags compose multiplicatively, so a row's tag is the conjunction of every predicate on its path, whatever the plan.
A filter over a row that is already tagged multiplies too: `FALSE` or `NULL` drops the row, because a row that does not exist has no error to report.
Join equivalences evaluated in a join closure, and temporal bounds, are predicates.

For this to hold across plans, scalar `AND` and `OR` must change so that `NULL` absorbs `ERROR`.
The absorption order of `AND` becomes `FALSE > NULL > ERROR > TRUE`, and `OR` becomes `TRUE > NULL > ERROR > FALSE`.
Today `ERROR` beats `NULL` (`src/expr/src/scalar/func/variadic.rs`).
With today's order, a row with `p = NULL` and `q = ERROR` is dropped when two separate filters apply `p` first, and tagged when one fused filter evaluates `p AND q`.
The optimizer moves conjuncts between scalar `AND` and predicate lists in both directions: `canonicalize_predicates` splits `AND`, filter fusion concatenates predicate lists, and predicate pushdown inlines maps into predicates.
The filter rule and scalar `AND` therefore cannot differ, and a filters-only rule is not an option.

The change is a refinement: the only evaluations that change go from `ERROR` to `NULL`.
In filter position both drop the row, so query results change only by no longer failing.
In map position, `SELECT a > 0 AND 1/b > 0` with `a` null and `b` zero returns `NULL` instead of an error.
PostgreSQL raises an error in that case, and Materialize already deviates in the same direction with `FALSE AND error = FALSE` in any order.
De Morgan still holds, and the scalar reducer's `AND` and `OR` rules use only laws the order does not affect.
The prior design doc of #36614 already chose `NULL AND ERROR = NULL`, while its Lean model encodes the old order, so the Lean model has to move.

A scalar-reducer bug has to be fixed first: `And(literal_err, x, …)` folds to the error even when another operand is literal `false` (CLU-137).
It introduces an error that evaluation would mask.

### Joins

* A pair's tag is the `or` of its inputs' tags, with the payload combined by `max`.
  A tainted row that matches nothing produces nothing, and an outer join's padding for it is tainted.
* A tainted row with a valid key arranges and matches on its data key like any row.
  The standing-query case needs exactly this: a tainted parameter row must still find its data rows.
* A row with an error in a column the plan uses as an arrangement key cannot be routed, so it stays a collection-scoped error.
  Its exact answer would be a tainted pair with every row of the other input, which collection scope expresses without the blowup.
* An equality the planner leaves to the join closure is a predicate on the pair, so it taints the pair.
* Whether an equality is an arrangement key or a closure predicate is the planner's choice, so an error in it is collection-scoped under one plan and row-scoped under another.
  This is the one permitted plan dependence, and it only ever reports an error where the other plan reports a tainted row or nothing.
  Elevating every equality column at the join inputs would remove it, but arranged inputs that the join does not drive, such as imported indexes, are only read for matched keys, so it cannot be implemented without a scan.
  This answers the corresponding open question of the cell-scoped design with no.

`RedundantJoin` and `SemijoinIdempotence` stay exact because `or` is idempotent: a removed input carries the same tag as the input it duplicates.
Predicate pushdown across a join is exact under the multiplier reading.
The Lean counterexample `filter_cross_pushdown_left_inexact` exists only because the #36614 filter keeps error mass on a false predicate.

Outer-join lowering computes the unmatched rows as a difference without a threshold, relying on the subtracted side being a subset of the base.
Under tags the subtracted side can carry a tag the base does not: a left row `l` and a tainted matching right row yield `(l, ok, +1)` and `(l, err, -1)`.
The anti-join therefore computes absent keys with the threshold rule below over the signed union of keys, which equals today's anti-join for untagged data.
`variadic_left` lowering needs the same change, or has to be disabled for inputs that may carry tags.

### Negate, threshold, and set operations

`Negate` negates every row, tagged or not, so `R EXCEPT ALL R` cancels its tainted rows.
Today `Negate` passes errors through unnegated, while an error produced after a negate carries the negated diff, and `distinct_errs` normalizes both, so today's behavior is a sign artifact rather than a semantics to keep.
The optimizer's negate rewrites, such as predicate and projection pushdown through `Negate`, need negation to be multiplication by `-1`.
Two independently tainted copies of one data row with the same payload on the two sides of a difference cancel.
Including the producing operator in the payload would keep independent errors apart, at the cost of a larger payload.

`Threshold` and `Distinct` group by the data columns and read the multiplicities per tag.
For a row `r` with ok count `n_ok` and tainted counts `n_p` per payload, `r` could have any multiplicity in `[n_ok + Σ min(0, n_p), n_ok + Σ max(0, n_p)]`.
Applying the monotone function `f` of the operator, `max(0, c)` for threshold and `min(1, max(0, c))` for distinct, gives `f(lo)` certain copies and `f(hi) - f(lo)` tainted copies with the smallest payload.
This is retraction-correct for the reason `distinct_errs` is: the output is a function of the accumulated state per key.

Places that omit a threshold because the subtracted side is a subset of the base have to be revisited.
`INTERSECT ALL` lowers to `lhs ∪ Negate(Threshold(lhs ∪ Negate(rhs)))` without an outer threshold, and with an ok row on the left and a tainted row on the right it produces a negative tainted row, so it needs the outer threshold.
`ThresholdElision` relies on `NonNegative`, whose superset check descends through `Filter`, so it has to stop at every operator that can add a tag.

### Reduce

A tainted input row poisons the aggregates of its group, not the existence of the group.
For a group key `k` with ok rows `O` and tainted rows `T`:

* With `T` empty, the output is as today.
* With `T` and `O` both non-empty, the output is one row with key `k`, every aggregate column an error cell, and an ok tag, since the group certainly exists.
* With `T` non-empty and `O` empty, the output is the same row with a tainted tag.

The output has multiplicity one per key in every case, and the payload is the `max` over the group.
`count(*)` follows the same rule, because its true value is unknown whenever `T` is non-empty.
One mechanism serves every reduce plan: after the key MFP, tainted rows go to a side reduction that keeps one payload per key, and the reduce output is combined with it by key.
The side arrangement is empty for error-free input.

### Top-k and window functions

Tainted rows keep their values, so they have a rank.
A row is certainly in the result if it is within the limit after discounting the tainted rows ranked above it, possibly in if enough tainted rows above it could be absent, and certainly out otherwise.
`LIMIT k` can therefore return up to `k` ok rows plus tainted rows.
Monotonic top-k plans, whose inputs never retract, use the coarse rule that any tainted row poisons its group.
Window functions lower to a reduce that sees the whole partition, so a partition with a tainted row gives every output row an error cell in the window-function column.

### Temporal filters

A tainted row expires with its bounds, because it is a row.
If a bound errors, the row is tainted over the widest interval the other bounds allow: a failing lower bound uses the input time, and a failing upper bound never expires.
Today a bound error is emitted at the input time and cancels when the input retracts, but it never expires, and the widest-interval rule keeps that behavior per row.

### Multiplicity and recursion

Tainted rows need no deduplication.
Diamond-shaped queries overflow today because every reader of a shared binding adds the binding's error collection to its own, so errors double per level while data does not.
A tag is multiplied through a join like data, so a tainted row's multiplicity equals its data row's, and overflows exactly when the data would.
The error collection keeps `distinct_errs` for the errors that stay collection-scoped.
A `WITH MUTUALLY RECURSIVE` round produces the same tainted row from the same input row, so tainted rows converge with the data, and the per-round error distinct stays for the error collection only.

## Boundaries

* Peeks, materialized views, sinks, and `COPY TO` elevate tagged rows to collection-scoped errors, as they elevate error cells.
  The elevated error is the tag's payload first, then error cells in column order.
* `SUBSCRIBE` elevates by default and poisons as today.
  With an opt-in option, it presents tagged rows as updates with a trailing `mz_error` column, `NULL` for ok rows, and error cells rendered as `NULL`.
  Tagged rows then retract like any update, which resolves the non-retractability of database-issues#5182 for row-scoped errors.
  Collection-scoped errors still poison.
* Materialized views can later persist tagged rows through a new `DataflowError` variant that carries the row and its error.
  `ProtoDataflowError` is a `oneof`, so the variant is an addition, but older binaries fail to decode it, so it needs a flag that flips only once rolling back is no longer possible.
* Peeks have no opt-in in the first steps, because `SELECT` has no option clause and pgwire has no per-row error channel.

## Access control

For a reader with `SELECT` on a view but not on what the view reads, per-row tags reveal, for each row, whether the conjunction of all predicates is `TRUE` or `ERROR`, the projected values of such rows, and the payload.
Today the reader learns the same classes by probing with predicates on visible columns, since `FALSE` masks errors, so tags change the bandwidth of the channel, not the set of facts a reader can learn.
The payload already contains values of columns the reader cannot see, today and under this design, which is the part that needs redaction.
Dataflows are shared across readers, so tagging cannot depend on the reader.
As hygiene, presentation can show tags only to readers that hold `SELECT` on every relation the dataflow reads, and elevate otherwise.

## Staging

1. The row-level error in rendered dataflows.
   Predicate errors in MFPs and join closures produce tags, joins combine them, and union, negate, `Let`, `LetRec`, and arrangements pass them as data.
   Reduce, top-k, table functions, and every boundary except the subscribe opt-in elevate.
   This unblocks standing queries without aggregation.
   The prototype implements this step, with `SUBSCRIBE ... WITH (INLINE ERRORS)` as the opt-in.
   It keeps today's `AND` order, and threshold treats the header as data rather than applying the interval rule.
2. The `AND` and `OR` order change, and reduce and threshold under the rules above, which `GROUP BY query_id` needs.
3. Top-k, window functions, and temporal bounds.
4. The row-carrying `DataflowError` variant, and subscribes over materialized views.

Payload redaction, peek opt-in, and the presentation policy can wait.

## Amendments to the cell-scoped design

* Rule 4 of the cell-scoped design lets an error mask `NULL` within one MFP, following today's `AND`.
  Under this design `NULL` absorbs `ERROR`, and the claim that predicate order cannot decide whether a row errors holds only once the order changes, since two filters separated by a join already disagree.
* The combined predicate error has to be the `max` of the errors, like scalar `AND`, not the first one in evaluation order.

## Open questions

* Threshold in the prototype treats tainted and ok copies of a row as different values, so `A EXCEPT B` with an ok row in `A` and a tainted copy in `B` returns the row without a taint, where the interval rule returns it tainted.
* Dictionary compression of arrangements cannot hold a row-level error.
* Whether to include the producing operator in the payload, so that independent errors do not cancel under `EXCEPT ALL`.
* `FoldConstants` must leave a `Join` unfolded when a constant input has an error datum in an equivalence column, as it does for an erroring `Map`.
* A mechanized model of the tagged collection, with the new `AND` order, proving filter fusion, pushdown across joins, and the threshold rule against an enumeration of resolutions.
