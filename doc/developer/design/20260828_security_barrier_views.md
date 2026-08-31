# Security barrier views

- Associated: [#38562](https://github.com/MaterializeInc/materialize/pull/38562)
  (prototype, object-level gates),
  [#38566](https://github.com/MaterializeInc/materialize/pull/38566)
  (prototype, security levels)

## The Problem

Materialize tells users that `SELECT` on a view is what governs access to it:

> To read from a views or a materialized views, you must have `SELECT`
> privileges on the view/materialized views. That is, having `SELECT`
> privileges on the underlying objects defining the view/materialized view is
> insufficient.

That statement is accurate about privilege checking, and it invites the
PostgreSQL pattern of using a view as a row-level access control boundary:

```sql
CREATE VIEW my_orders AS SELECT * FROM orders WHERE tenant = current_user;
GRANT SELECT ON my_orders TO tenant_role;
```

The privilege half of this works. `Plan::Select` requires read privileges only
on the objects the query itself names, and `current_user` inside a view body is
resolved against the *querying* session, so the view behaves like a PostgreSQL
view with definer privileges and invoker identity.

The optimizer half does not. A predicate supplied by the reader is pushed
across the view boundary and evaluated *before* the view's own filter, so a
reader can aim a fallible expression at rows they are not allowed to see and
read the hidden values back out of the resulting error message.

PostgreSQL solved this with `security_barrier` views. Materialize has no
equivalent.

### Current exposure

This is not hypothetical. Modeling the situation directly against
`optimize_dataflow_filters_inner`, with `#0` a secret column and `#1` the
tenant column:

| view   | plan                          |
| ------ | ----------------------------- |
| `mine` | `Filter (#1 = 7)` over `Get orders` |
| `q`    | `Filter ((100 / #0) > 0)` over `Get mine` |

produces:

```
--- `mine` after cross-view filter pushdown ---
Filter (#1 = 7) AND ((100 / #0) > 0)
  Get orders

--- predicates propagated to the `orders` source import ---
["(#1 = 7)", "((100 / #0) > 0)"]

--- MFP evaluation order inside `mine` ---
[ "@1: ((100 / #0) > 0)",
  "@2: (#1 = 7)" ]
```

Three separate things go wrong:

1. The reader's predicate is spliced into the view's own plan, becoming a
   sibling of the tenant filter rather than staying above it.
2. It propagates all the way to the `orders` source import, where it will be
   handed to persist filter pushdown and evaluated against every row of the
   base collection.
3. Inside the resulting `MapFilterProject`, predicates are ordered by the
   column position they first reference, so the reader's predicate on `#0` is
   scheduled *ahead* of the tenant filter on `#1`.

Two mechanisms perform the boundary crossing. `inline_views` rewrites a
singly-referenced view into a `Let` binding, after which
`PredicatePushdown::push_into_let_binding` moves the reader's predicate into
the binding's value and deletes it from the consumer.
`optimize_dataflow_filters_inner` performs the same propagation for views that
survive inlining. Everywhere else, a global `Get` is opaque to predicate
pushdown: the transform records predicates at a `Get` but explicitly cannot
delete them from the enclosing `Filter`.

Turning this into an oracle is easy, because Materialize's error messages embed
the offending value:

```
invalid input syntax for type integer: "<the actual hidden value>"
"<the actual hidden value>" bigint out of range
```

So `SELECT * FROM my_orders WHERE ssn::int > 0` exfiltrates a foreign tenant's
`ssn` verbatim in a single query, and `WHERE 1 / (secret - 42) > 0` gives a
clean binary-search oracle over any numeric column.

Materializing the view is not an available mitigation. `ExprPrepMaintained`
rejects unmaterializable functions, so a view whose predicate references
`current_user` cannot be indexed or materialized. Exactly the views that need
a barrier are the ones that are always inlined into the reader's dataflow.

## Success Criteria

1. A view can be marked such that no reader-supplied expression is evaluated
   against a row the view's own filter would have excluded.
2. The guarantee holds against the optimizer, not merely against the plans it
   happens to produce today. A new transform that crosses the view boundary
   should have to opt in rather than silently defeat the barrier.
3. Predicates that cannot leak still cross the boundary, so the common
   selective filters keep reaching persist pushdown and index lookups.
4. The default behavior of existing views does not change.

## Out of Scope

- Row-level security policies attached to tables. This design covers views
  only.
- `security_invoker` views. Materialize's privilege model is already
  definer-style and this design does not change it.
- Timing and introspection side channels. A barrier view's base scan still
  runs on the reader's cluster, and `mz_introspection` is `PUBLIC_SELECT`, so
  operator-level record counts and query latency remain observable. PostgreSQL
  has the same class of hole and does not address it either. See
  [Open questions](#open-questions).
- Making the barrier the default for all views. Considered and rejected; see
  [Why the option is opt-in](#why-the-option-is-opt-in).

## Solution Proposal

The SQL surface and the exposure analysis below are common to both candidate
mechanisms. The mechanisms themselves are presented side by side under
[Mechanisms](#mechanisms), with a prototype for each. This document does not
pick between them: that is a call for the team that owns the optimizer.

### Syntax

```sql
CREATE VIEW my_orders WITH (SECURITY BARRIER) AS
    SELECT * FROM orders WHERE tenant = current_user;
```

`WITH (SECURITY BARRIER = false)` is accepted and is the default. The option
lands on `ViewDefinition` so it prints between the column list and `AS`,
matching PostgreSQL and round-tripping through `SHOW CREATE VIEW` via
`create_sql`.

### Leakproofness

PostgreSQL needs `LEAKPROOF` because it has user-defined functions that can
leak through `RAISE NOTICE`, error text, or a dishonest `COST`. Marking a
function `LEAKPROOF` is therefore a superuser trust decision recorded in
`pg_proc`.

Materialize has no user-defined functions, so every function in a predicate is
a builtin, and the only value-dependent channel a builtin has is its error.
Leakproof therefore reduces to infallible, and `MirScalarExpr::could_error()`
already answers exactly that question. Its defaults are fail-safe:
`LazyUnaryFunc::could_error` returns `true` unless a function overrides it, and
`EagerUnaryFunc` derives it from whether the Rust signature returns a `Result`.

This is a strictly better foundation than `pg_proc.proleakproof`. It is
type-derived and compiler-checked rather than asserted by a human, and it
cannot be subverted by a user defining their own function.

The cost of the barrier is correspondingly small. `eq` is declared
`fn eq(...) -> bool`, not `-> Result<...>`, so `=` is automatically leakproof,
as are `<`, `>`, `AND`, `OR`, and `IS NULL`. Those are precisely the predicates
that persist part-pruning and `LiteralConstraints` index lookups can exploit,
so the pushdown that actually pays for itself still crosses the barrier. What
stops at the boundary is fallible expressions, which pushdown would not have
been able to use for pruning anyway.

## Mechanisms

Two mechanisms implement the same surface and close the same exposure. They
differ in where the guarantee lives, what it costs, and how they fail. Each has
a prototype.

Both need the barrier set at optimization time. It is optimizer-only state, so
it belongs on `TransformCtx`, the struct that already threads arguments through
all transforms, rather than on `DataflowDescription`, which is part of the
compute protocol. `DataflowBuilder` collects it during import, which is the only
point at which the optimizer holds the catalog entry.

### A. Object-level gates

Prototype: [#38562](https://github.com/MaterializeInc/materialize/pull/38562).
This is PostgreSQL's pre-9.5 model: mark the view, then decline to dissolve its
boundary.

`inline_views` skips a barrier view, so it stays a distinct `objects_to_build`
entry referenced through a global `Get`. Every per-object transform already
treats that as opaque, and no `Let` binding is formed for
`PredicatePushdown::push_into_let_binding` to push into.
`optimize_dataflow_filters_inner` then applies only leakproof predicates to a
barrier. A blocked predicate never enters the view's plan, so it never
propagates onward to the view's inputs either.

Two gates, both in `optimize_dataflow`. Nothing else in the pipeline needs to
know the concept exists, because the boundary is between dataflow objects and
the optimizer already respects those.

Worth being precise about what "the boundary holds" means here, because it is
not isolation. The view and its consumer remain two objects in one dataflow, and
`optimize_dataflow_relations` runs the whole transform pipeline over each
object separately, so no transform ever sees both plans at once. Three
cross-object passes still run: `optimize_dataflow_filters`, which the gate
restricts to leakproof predicates, and `optimize_dataflow_demand` and
`optimize_dataflow_monotonic`, which are not gated at all. Column pruning and
monotonicity therefore cross a barrier freely, which is safe because neither
decides whether a row is produced.

### B. Security levels on predicates

Prototype: [#38566](https://github.com/MaterializeInc/materialize/pull/38566).
This is PostgreSQL 9.5 and later, and it is also the mechanism row-level
security would need.

`Filter` carries `Vec<Predicate>`, an expression plus the level it was
introduced at. `inline_views` raises the consumer's non-leakproof predicates a
level before splicing a barrier in, so the view's body is optimized jointly with
its consumer's while ordering stays constrained. Incrementing rather than
assigning a fixed level is what makes nested barriers work.

Three seams enforce it: **movement**, where a levelled predicate may not sink
below a lower level nor be reported at a `Get`; **derivation**, where an
equivalence class is seeded only from leakproof or unconstrained predicates,
since a derived qual carries no level; and **ordering**, where a levelled
predicate must be evaluated after every lower-level one.

### Comparison

The difference underneath every row below is that A rides a boundary Materialize
already has, while B introduces a new one.

A dataflow is already a set of objects that are optimized independently, and an
object referenced from another is opaque by construction. A does not add a rule,
it declines to dissolve a boundary that exists anyway, and because objects are a
runtime concept that boundary is represented at every layer: the mid-level plan,
the physical plan, and the running dataflow.

B inlines the view, so the boundary is gone, and asserts in its place an
invariant that exists only during planning. By the time a plan reaches the
physical layer all filtering has been fused into one operator, so there is no
filter left to order, which is why the ordering has to be pushed down into the
physical layer and the protocol that ships plans to compute.

| | A. Object gates | B. Levels |
| --- | --- | --- |
| What the optimizer may do across the view | the two plans stay distinct objects, so no transform sees both at once; leakproof predicates, column demand, and monotonicity still cross | the two plans merge into one, so every transform applies to both; only evaluation order is constrained |
| Unit of the constraint | the whole view | one predicate relative to another |
| Where the guarantee lives | structural: nothing from a consumer can enter a producer's plan | by rule, at each seam that moves, orders, or derives predicates |
| Failure mode of a missed site | costs an optimization | silently produces an insecure plan |
| Reaches LIR / compute protocol | no | yes, once `MapFilterProject` carries the level |
| Perf, barrier views | view is not inlined: no cross-boundary folding, an extra collection and often an arrangement per boundary, no fast-path peek | inlining preserved; a related spike measured zero plan delta |
| Perf, non-barrier views | none | none: every text plan in the `EXPLAIN` corpus is byte-identical |
| Generalizes to row-level security | no | yes |
| Prototype status | complete and verified end to end | complete and verified end to end |

Row four is the one that outlives this design. Under A the safe behavior is the
default: a transform that has never heard of security barriers cannot reach
across an object boundary, because reaching across is not something a transform
can do, so forgetting costs an optimization. Under B the optimizing behavior is
the default: a transform that has never heard of levels moves predicates freely,
because moving predicates is its job, so forgetting costs the guarantee and
nothing reports it.

Row eight is why B was explored at all. Row-level security means a table's own
policy predicates must be evaluated before the user's, and there is no object
boundary at a table scan to hang that on, so A cannot express it at any price.
B's per-predicate ordering is the shape row-level security needs.

The measured perf gap is the whole reason B exists. Applying barriers to every
user view under A changes 327 lines of the `EXPLAIN` corpus, and running the
same experiment with A's predicate gate disabled produces a byte-identical diff,
so all of that cost comes from declining to inline rather than from the security
property. One case regresses from `Constant <empty>` to a full differential join
with two arrangements over a base collection, to compute a provably empty result.

B's cost lands in a different place. Ordering has to be enforced inside a single
`MapFilterProject`, because LIR has no `Filter` operator and lowering asserts
every filter was extracted into one. Two cheaper encodings were tried and both
failed: putting the level in a predicate's position is unsound, since
`memoize_expressions` indexes `expressions` by position, and declining to fuse
across levels leaves a `Filter` that cannot lower. So `MapFilterProject` carries
the level, which puts it in LIR and on the compute protocol. The runtime does
not need it: `SafeMfpPlan` already evaluates predicates in vector order, so the
sort discharges the constraint. It crosses because `MapFilterProject` is one
generic type shared by both plan levels.

The finished prototype touches ~90 sites across 24 files. Every one is a
predicate conversion that had to state whether it was creating a new
unconstrained predicate or re-applying an existing one, because there is no
blanket conversion from an expression to a predicate. That is what surfaced
three real level-dropping bugs, in `into_map_filter_project`,
`literal_constraints`, and `canonicalize_mfp`, each of which would have compiled
silently under a blanket conversion. It is also the standing cost: the same
discipline applies to every future transform that touches predicates, and
nothing enforces it but the review.

Two smaller costs specific to B: `Predicate` is 104 bytes against
`MirScalarExpr`'s 96, and the serialized MIR nests `{expr, level}`, changing
`EXPLAIN AS JSON` and requiring an expression-cache format bump.

### What this fixes

End to end, through the real optimizer, with mechanism A. Given

```sql
CREATE TABLE orders (tenant text, secret text, amount int);
CREATE VIEW plain_orders   AS SELECT * FROM orders WHERE tenant = 'alice';
CREATE VIEW barrier_orders WITH (SECURITY BARRIER) AS
    SELECT * FROM orders WHERE tenant = 'alice';
```

the unprotected view hands the reader's fallible expression to the base
collection:

```
EXPLAIN SELECT * FROM plain_orders WHERE amount / (length(secret) - 3) > 0;

Explained Query:
  Filter (#0{tenant} = "alice") AND ((#2{amount} / (char_length(#1{secret}) - 3)) > 0)
    ReadStorage materialize.public.orders

Source materialize.public.orders
  filter=((#0{tenant} = "alice") AND ((#2{amount} / (char_length(#1{secret}) - 3)) > 0))
```

The barrier view does not:

```
EXPLAIN SELECT * FROM barrier_orders WHERE amount / (length(secret) - 3) > 0;

Explained Query:
  Filter ((#2{amount} / (char_length(#1{secret}) - 3)) > 0)
    ReadGlobalFromSameDataflow materialize.public.barrier_orders

materialize.public.barrier_orders:
  Filter (#0{tenant} = "alice")
    ReadStorage materialize.public.orders

Source materialize.public.orders
  filter=((#0{tenant} = "alice"))
```

The view survives as its own object, read through
`ReadGlobalFromSameDataflow`, the division sits above it, and the tenant filter
is the only thing reaching `orders`.

A leakproof predicate still crosses, and still reaches the source import where
persist pruning can use it:

```
EXPLAIN SELECT * FROM barrier_orders WHERE amount = 42;

Explained Query:
  Filter (#2{amount} = 42)
    ReadGlobalFromSameDataflow materialize.public.barrier_orders

materialize.public.barrier_orders:
  Filter (#0{tenant} = "alice") AND (#2{amount} = 42)
    ReadStorage materialize.public.orders

Source materialize.public.orders
  filter=((#0{tenant} = "alice") AND (#2{amount} = 42))
```

## Minimal Viable Prototype

Two prototypes, one per mechanism, so that they can be compared rather than
argued about.

**A, object-level gates**
([#38562](https://github.com/MaterializeInc/materialize/pull/38562)) is complete
and verified end to end. The optimizer change proper is two gates and one
predicate in `src/transform/src/dataflow.rs`; the rest is option plumbing.
`src/transform/tests/test_security_barrier.rs` asserts the blocked, admitted,
and unprotected cases at the `optimize_dataflow_filters_inner` seam, including a
test that pins the current exposure so a regression is visible.
`test/sqllogictest/security_barrier.slt` covers the SQL surface and the
resulting `EXPLAIN` plans. The existing `EXPLAIN` and privilege suites pass
unchanged, confirming the default path is untouched.

**B, security levels**
([#38566](https://github.com/MaterializeInc/materialize/pull/38566)) is also
complete. 1768 sqllogictest assertions pass, and no text plan in the `EXPLAIN`
corpus changes: only the JSON goldens move, for the serialization shape, which
implies an expression-cache format bump. The guarantee is pinned by physical
plans over a table whose column order would otherwise schedule the reader's
fallible cast ahead of the tenant filter:

```
plain   filter=((text_to_integer(#0{secret}) > 0) AND (#1{tenant} = "alice"))
barrier filter=((#1{tenant} = "alice") AND (text_to_integer(#0{secret}) > 0))
```

Both are fully inlined, so ordering is the only difference. B is built on A's
branch, so it inherits the SQL surface and tests; only the mechanism differs.

Deliberately left out of both:

- An `mz_views` catalog column reporting the flag. `SHOW CREATE VIEW` already
  reflects it through `create_sql`.
- `ALTER VIEW ... SET (SECURITY BARRIER)`.
- Documentation under `doc/user/`.

## Why the option is opt-in

The option defaults off. That is a recommendation this document does make, and
it is independent of which mechanism is chosen: both pay the cost below, and
neither can avoid it.

### A plan that a barrier makes more expensive

Put the security filter and the reader's predicate on different tables, so that
the reader's predicate would otherwise be pushed into a scan the security filter
does not cover.

```sql
CREATE TABLE acct (tenant text, id int);
CREATE TABLE det  (id int, secret text);

CREATE VIEW my_det AS
  SELECT det.id, det.secret
  FROM acct JOIN det ON acct.id = det.id
  WHERE acct.tenant = current_user;

SELECT * FROM my_det WHERE secret::int > 0;
```

Without a barrier the cast is applied at the read of `det`:

```
Source materialize.public.det
  filter=((#0{id}) IS NOT NULL AND (text_to_integer(#1{secret}) > 0))
```

so the arrangement on `det` only ever receives rows that passed it. With a
barrier the cast moves into the join's per-match closure and leaves the source
filter:

```
Join::Linear
  linear_stage[0]
    closure
      filter=((text_to_integer(#1{secret}) > 0))
...
Source materialize.public.det
  filter=((#0{id}) IS NOT NULL)
```

`det` is now arranged in full rather than pre-filtered. Arrangement size is the
dominant memory cost in a Materialize dataflow, so the penalty is the
selectivity of the reader's predicate: a cast that admits one row in a hundred
costs roughly a hundredfold on that arrangement.

Two things about this example are worth stating plainly.

**The cost is inherent, not an artifact of either mechanism.** Filtering `det`
by the reader's cast before the join would evaluate that cast against rows
belonging to every tenant, which is exactly the disclosure the barrier exists to
prevent. There is no correct plan that filters `det` early. The cheap plan is
cheap because it is wrong. Mechanism A pays the same cost and adds to it, since
it also declines to inline.

**It is not a scan-volume cost.** Persist filter pushdown already treats a
fallible expression conservatively, because it must not discard a part whose
interior rows would error, so such a predicate was never driving much part
pruning. What a barrier gives up is early filtering ahead of arrangements and
joins, which is a narrower and better-defined thing to reason about.

### The case for opt-in

**The cost has a recognizable shape.** It bites when a reader's predicate is
fallible, selective, and sits above a join or an arrangement. That is a real
pattern, not a corner case, and a user who hits it should be able to trace the
regression to a decision somebody made rather than to a property the system
applies invisibly.

**Attribution matters more than the average.** Applying barriers to every user
view changes nothing in the `EXPLAIN` corpus, so the average cost may well be
near zero. But the corpus does not contain the shape above, and an average is
the wrong statistic for a cost that is concentrated. A query that silently loses
early filtering because a cast happened to land above a join is very hard to
explain to the person who wrote it.

**The marker means something beyond the optimizer.** It records which views are
access-control boundaries, which is information a reader of the schema wants and
which no amount of default-on behaviour supplies. It also scopes what we have to
defend: "declared views enforce ordering" is a claim we can state and test,
where "no view ever evaluates a reader's fallible predicate early" is a
whole-system property about every view, forever.

**PostgreSQL has kept it opt-in since 9.2**, and its stated reason is the same
category of cost. Their version is worse, because a non-leakproof qual cannot
become an index qual and a row store then falls back to a sequential scan, which
Materialize's streaming filters do not have an analogue for. But the direction
of their conclusion survives the difference.

### The argument against, and what to do about it

The failure modes are not symmetric. Forgetting the option is silent data
disclosure. Enabling it unnecessarily is a slower query. Anyone arguing for
default-on is making a sound argument, and the measurements above do not refute
it.

The answer to that is not to pay the cost everywhere, but to make forgetting
detectable. A view is being used as an access boundary exactly when some role
holds `SELECT` on it and lacks `SELECT` on what it reads. That is a catalog
query, not an optimizer change, and it can back a warning, a linting view, or an
`mz_internal` relation listing views that look like boundaries but are not
barriers. That closes the asymmetry at a fraction of the cost of default-on, and
it is worth doing whichever mechanism is chosen.

## When to enable a barrier

**Enable it when the view is the access boundary**: the reader holds `SELECT`
on the view and not on what it reads, and the rows it filters out are rows the
reader must not learn about. That is the only case the feature is for. A view
that merely tidies up a query the reader could have written themselves does not
need one.

**The barrier is free when** the view is a materialized view, or is indexed on
every cluster its readers use, because `import_into_dataflow` resolves an index
or a materialized view before it ever considers a view plan and so no mechanism
engages at all. It is also cheap whenever readers filter with comparisons: `=`,
`<`, `>`, `AND`, and `IS NULL` are all infallible, so they are leakproof and
cross the barrier untouched.

**The barrier is expensive when** a reader's fallible predicate is selective and
sits above a join or an arrangement, per the example above. Under mechanism A it
is additionally expensive when the view sits partway up a stack of other views,
because the whole stack loses joint optimization, and when readers issue point
lookups, because a barrier view occupies `objects_to_build[0]` and disqualifies
the fast-path peek.

**Materializing is not a substitute**, for three reasons. An index only helps on
the cluster that holds it, and a reader chooses their own cluster, so an
attacker runs the query somewhere the index is absent and the view is inlined
again. A materialized view that is a replacement target is imported from its
view definition rather than its shard, and is inlined. And a view whose
predicate calls `current_user` can be neither indexed nor materialized, because
`ExprPrepMaintained` rejects unmaterializable functions. What a barrier adds
over materializing is that the guarantee stops depending on where the query
runs.

## Alternatives

The two candidate mechanisms are not alternatives to each other in this section;
they are in [Mechanisms](#mechanisms). What follows are approaches rejected
before either was prototyped.

**Make every view a barrier.** Correct by default and immune to a user
forgetting the option. Rejected, for the reasons in
[Why the option is opt-in](#why-the-option-is-opt-in): the cost is concentrated
rather than average, so applying it everywhere makes a real regression
unattributable. The measurements are there too, including that mechanism A moves
327 lines of the `EXPLAIN` corpus under this default while B moves none, which
is what motivated exploring B in the first place.

**Wrap the predicate in an opaque marker instead of carrying a level.** A
`SecurityFence` unary function that the optimizer refuses to move. Prototyped:
it recovers all of A's cost and needs only 7 files, because an unrecognized
wrapper is inert by default rather than requiring every transform to opt in. It
was abandoned because wrapping makes a predicate syntactically unrecognizable,
which is fail-safe where a transform is optional but fail-stop where its output
is required. Temporal filters break outright: `mz_now()` is non-leakproof, so it
gets wrapped, and `MfpPlan` then rejects it as an unsupported temporal
predicate. Carrying the level beside the expression, as B does, leaves the
expression untouched and does not have this problem.

**Document that views are not a security boundary.** Legitimate, and strictly
cheaper than either mechanism. It is the right answer if we conclude that
view-based row filtering is not a pattern we intend to support. It conflicts
with what the RBAC documentation currently implies, so if we choose it we should
say so explicitly rather than by omission.

**Reject fallible expressions in predicates over any view.** Blocks the error
channel without any optimizer changes, but it breaks a large amount of
legitimate SQL (`::int` casts, division) and still leaves the ordering problem
for any future non-error channel.

## Open questions

- Do we want to support view-based row-level access control at all? The whole
  design is contingent on that. If yes, this is table stakes and the current
  behavior is a security bug. If no, the honest move is to document that views
  are not a security boundary and close this out.
- Which mechanism? Both prototypes work and close the same exposure. A fails
  closed and confines the concept to two gates, but costs real performance on
  barrier views beyond the shared cost. B preserves that performance and is the
  mechanism row-level security would need, but puts the concept on the compute
  protocol and carries a standing obligation: every future transform that
  touches predicates has to preserve the level, and nothing enforces that but
  review. This document deliberately does not pick.
- The default is settled in this document and the mechanism is not. If the team
  disagrees with opt-in, the argument to engage is in
  [The argument against](#the-argument-against-and-what-to-do-about-it), and the
  detection idea there is the part worth keeping either way.
- `MirScalarExpr::could_error` is currently maintained to keep persist filter
  pushdown correct. Promoting it to a security boundary means a wrong
  `could_error = false` becomes a vulnerability rather than a performance bug.
  Do we want an explicit policy and a test that pins the classification, or is
  the type-derived default sufficient?
- Timing and `mz_introspection` remain oracles for the cardinality of the
  pre-filter scan, since a barrier view is still inlined into a dataflow on the
  reader's cluster. Is that acceptable, as it is in PostgreSQL, or does a
  serious answer require running the view on a cluster the reader does not have
  introspection access to?
- Should a barrier view be allowed to depend on a non-barrier view that reads
  the same protected table? Nothing prevents it, and it is a plausible way to
  build a barrier that does not actually protect anything.
