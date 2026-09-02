# Security barrier views

- Associated: [#38568](https://github.com/MaterializeInc/materialize/pull/38568)
  (prototype),
  [#38562](https://github.com/MaterializeInc/materialize/pull/38562)
  (prototype of the rejected alternative)

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

Mark the view, then carry a security level on each predicate and constrain
movement and evaluation order by level. This is PostgreSQL 9.5 and later, and it
is also the mechanism row-level security would need.

The alternative considered and rejected was to refuse to inline a barrier view,
which is PostgreSQL's pre-9.5 model. It closes the same exposure and is simpler,
but it costs real performance and cannot generalize. See
[Alternatives](#alternatives).

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

### How it works

The barrier set is needed at optimization time. It is optimizer-only state, so
it belongs on `TransformCtx`, the struct that already threads arguments through
all transforms, rather than on `DataflowDescription`, which is part of the
compute protocol. `DataflowBuilder` collects it during import, the only point at
which the optimizer holds the catalog entry.

`Filter` carries `Vec<Predicate>`, an expression plus the level it was
introduced at. `inline_views` raises the consumer's non-leakproof predicates a
level before splicing a barrier in, so the view's body is still optimized jointly
with its consumer's while ordering stays constrained. Incrementing rather than
assigning a fixed level is what makes nested barriers compose: each inlining
lifts everything already merged, so a barrier's own predicates stay below every
predicate written above it.

Three seams enforce it:

- **Movement.** A levelled predicate may not sink below a lower level, and may
  not be reported at a `Get`, where the level would be lost crossing into
  another object. `PredicatePushdown` splits levelled predicates into a `Filter`
  it then leaves alone. Everything below that split sees only level-0
  predicates, which is what makes it sound for the rest of the transform to keep
  working in bare expressions.
- **Ordering.** `MapFilterProject` sorts by level first. Evaluation walks its
  predicate list in order and stops at the first failure, so list order is
  evaluation order and the sort is where the constraint is discharged.
- **Derivation.** An equivalence class is seeded only from predicates that are
  leakproof or unconstrained, since a derived qual carries no level of its own.

Ordering has to be enforced inside a single `MapFilterProject` because LIR has no
`Filter` operator and lowering asserts every filter was extracted into one. Two
cheaper encodings were tried and both failed: putting the level in a predicate's
position is unsound, since `memoize_expressions` indexes `expressions` by
position, and declining to fuse across levels leaves a `Filter` that cannot
lower. So `MapFilterProject` carries the level, which puts it in LIR and on the
compute protocol. The runtime does not need it, because `SafeMfpPlan` already
evaluates predicates in vector order. It crosses because `MapFilterProject` is
one generic type shared by both plan levels.

Two smaller costs: `Predicate` is 104 bytes against `MirScalarExpr`'s 96, and
the serialized MIR nests `{expr, level}`, which changes `EXPLAIN AS JSON` and
requires an expression-cache format bump.

### Residual risk

The residual risk lies entirely in code that does not exist yet. If a **new
transform mishandles a level it may generate an insecure plan, and nothing
reports it**. There is no blanket conversion from an expression to a predicate,
so constructing one requires naming a level: `Predicate::unconstrained` or
`Predicate::at_level`. And `level` is private with `raise` as its only mutator,
so no code can lower one. What a new author can miss is therefore not the
mechanism but the contract, which is recorded on the `mz_expr::predicate`
module: where a levelled predicate may be relocated to, what level a derived
predicate inherits, and what a new predicate-bearing operator owes. The first is
expressible as a plan-tree invariant and belongs in `Typecheck`; the second has
no mechanical check; the third would apply to any mechanism.

### What this fixes

End to end, through the real optimizer. Given

```sql
CREATE TABLE orders (tenant text, secret text, amount int);
CREATE VIEW plain_orders   AS SELECT * FROM orders WHERE tenant = 'alice';
CREATE VIEW barrier_orders WITH (SECURITY BARRIER) AS
    SELECT * FROM orders WHERE tenant = 'alice';
```

the unprotected view hands the reader's fallible expression to the base
collection:

```
EXPLAIN OPTIMIZED PLAN FOR SELECT * FROM plain_orders WHERE amount / (length(secret) - 3) > 0;

Explained Query:
  Filter (#0{tenant} = "alice") AND ((#2{amount} / (char_length(#1{secret}) - 3)) > 0)
    ReadStorage materialize.public.orders

Source materialize.public.orders
  filter=((#0{tenant} = "alice") AND ((#2{amount} / (char_length(#1{secret}) - 3)) > 0))
```

The barrier view does not. The view is still inlined, so there is one plan rather
than two, and the levels are visible on the `Filter`:

```
EXPLAIN OPTIMIZED PLAN FOR SELECT * FROM barrier_orders WHERE amount / (length(secret) - 3) > 0;

Explained Query:
  Filter (#0{tenant} = "alice") AND ((#2{amount} / (char_length(#1{secret}) - 3)) > 0) [levels: [0, 1]]
    ReadStorage materialize.public.orders

Source materialize.public.orders
  filter=((#0{tenant} = "alice"))
```

That plan shows movement being blocked: only the tenant filter reaches the
source. Ordering shows up in the physical plan, where the two predicates fuse
into one operator. Over a table whose column order would otherwise schedule the
reader's cast first, `t(secret, tenant)`:

```
plain    filter=((text_to_integer(#0{secret}) > 0) AND (#1{tenant} = "alice"))
barrier  filter=((#1{tenant} = "alice") AND (text_to_integer(#0{secret}) > 0))
```

Both are fully inlined, so the order is the only difference. `secret` is `#0`
and `tenant` is `#1`, so position alone would evaluate the cast first. The level
outranks position.

A leakproof predicate carries no level, so it still crosses and still reaches
the source import where persist pruning can use it:

```
EXPLAIN OPTIMIZED PLAN FOR SELECT * FROM barrier_orders WHERE amount = 42;

Explained Query:
  Filter (#0{tenant} = "alice") AND (#2{amount} = 42)
    ReadStorage materialize.public.orders

Source materialize.public.orders
  filter=((#0{tenant} = "alice") AND (#2{amount} = 42))
```

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

**The cost is inherent, not an artifact of the mechanism.** Filtering `det`
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

**Default-on turns a query cost into an upgrade cost.** The expression cache is
keyed on build version, so every plan is re-derived on every version bump. What
makes that safe today is that the new version almost always arrives at the same
plan, so a replica's memory requirement is the one it already met. Zero-downtime
upgrade then runs both generations until the new one has re-hydrated, which
commits the headroom that would absorb a change.

Enabling barriers for every view would re-derive affected plans into ones that
need more memory, per the example above, at the point in an environment's life
when the least is spare. A replica that cannot fit the new plan cannot finish
re-hydrating, and an upgrade whose new generation cannot finish re-hydrating does
not cut over. That failure is not a slow query, it is an environment that cannot
be upgraded, discovered during the upgrade. Opt-in avoids it by construction: an
existing view's plan does not change unless somebody changes the view.

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
sits above a join or an arrangement, per the example above. That is the only
case, because the view is still inlined and everything else about planning it is
unchanged.

**Materializing is not a substitute**, for three reasons. An index only helps on
the cluster that holds it, and a reader chooses their own cluster, so an
attacker runs the query somewhere the index is absent and the view is inlined
again. A materialized view that is a replacement target is imported from its
view definition rather than its shard, and is inlined. And a view whose
predicate calls `current_user` can be neither indexed nor materialized, because
`ExprPrepMaintained` rejects unmaterializable functions. What a barrier adds
over materializing is that the guarantee stops depending on where the query
runs.

## Minimal Viable Prototype

[#38568](https://github.com/MaterializeInc/materialize/pull/38568) is complete
and verified end to end. 1768 sqllogictest assertions pass across the `EXPLAIN`
corpus, privileges, joins, subqueries, and the barrier tests, and no text plan in
the `EXPLAIN` corpus changes: only the JSON goldens move, for the serialization
shape.

The guarantee is pinned by the plans in
[What this fixes](#what-this-fixes), including the physical-plan pair over a
table whose column order would otherwise schedule the reader's cast first. The
ordering rule is also stated once, declaratively, in
`src/expr/tests/test_security_levels.rs` and checked against arbitrary predicate
lists, verified red before green: removing the level from the sort key fails
three of the five properties.

The rejected alternative is prototyped separately at
[#38562](https://github.com/MaterializeInc/materialize/pull/38562), so the two
can be compared rather than argued about.

Deliberately left out:

- An `mz_views` catalog column reporting the flag. `SHOW CREATE VIEW` already
  reflects it through `create_sql`.
- `ALTER VIEW ... SET (SECURITY BARRIER)`.
- Documentation under `doc/user/`.

## Alternatives

**Object-level gates: refuse to inline a barrier view.** PostgreSQL's pre-9.5
model, and the closest alternative. `inline_views` skips a barrier view so it
stays a distinct `objects_to_build` entry referenced through a global `Get`,
which every per-object transform already treats as opaque, and
`optimize_dataflow_filters_inner` then applies only leakproof predicates to it.
Two gates, both in `optimize_dataflow`, and nothing else in the pipeline needs to
know the concept exists. Prototyped and complete at
[#38562](https://github.com/MaterializeInc/materialize/pull/38562).

It is the better mechanism on one axis: the guarantee is structural rather than
by rule. A transform cannot reach across an object boundary because reaching
across is not an operation a transform has, so forgetting costs an optimization
rather than the guarantee. It also never reaches LIR or the compute protocol.

It was rejected on two counts. **Performance:** applying barriers to every user
view moves 327 lines of the `EXPLAIN` corpus, and running the same experiment
with its predicate gate disabled produces a byte-identical diff, so all of that
cost comes from declining to inline rather than from the security property. One
case regresses from `Constant <empty>` to a full differential join with two
arrangements over a base collection, to compute a provably empty result. Barrier
views also lose cross-boundary folding, gain an extra collection and often an
arrangement per boundary, and no longer qualify for the fast-path peek, since a
barrier view occupies `objects_to_build[0]`.

**Generality:** it cannot express row-level security at any price. RLS means a
table's own policy predicates must be evaluated before the user's, and there is
no object boundary at a table scan to hang a gate on. Per-predicate ordering is
the shape RLS needs.

**Make every view a barrier.** Correct by default and immune to a user
forgetting the option. Rejected, for the reasons in
[Why the option is opt-in](#why-the-option-is-opt-in): the cost is concentrated
rather than average, so applying it everywhere makes a real regression
unattributable. Note that the cost is concentrated under this proposal but
pervasive under object-level gates, which move 327 lines of the `EXPLAIN` corpus
under the same default.

**Wrap the predicate in an opaque marker instead of carrying a level.** A
`SecurityFence` unary function that the optimizer refuses to move. Prototyped:
it recovers the same performance and needs only 7 files, because an unrecognized
wrapper is inert by default rather than requiring every transform to opt in. It
was abandoned because wrapping makes a predicate syntactically unrecognizable,
which is fail-safe where a transform is optional but fail-stop where its output
is required. Temporal filters break outright: `mz_now()` is non-leakproof, so it
gets wrapped, and `MfpPlan` then rejects it as an unsupported temporal
predicate. Carrying the level beside the expression leaves the expression
untouched and does not have this problem.

**Document that views are not a security boundary.** Legitimate, and strictly
cheaper than any mechanism. It is the right answer if we conclude that
view-based row filtering is not a pattern we intend to support. It conflicts
with what the RBAC documentation currently implies, so if we choose it we should
say so explicitly rather than by omission.

**Remove the offending value from error messages.** The most frequently
proposed fix, and the most intuitive: the message literally contains the SSN, so
stop printing it. It does not work, because the channel is the error's
*existence*, not its content.

A predicate that fails conditionally turns any comparison into a one-bit oracle,
and the message carries no data at all:

```sql
-- error means some hidden row matches
SELECT * FROM my_orders
WHERE CASE WHEN secret LIKE '123%' THEN 1/0 ELSE 1 END = 1;
ERROR:  Evaluation error: division by zero

-- a prefix that matches nothing returns normally
SELECT * FROM my_orders
WHERE CASE WHEN secret LIKE '555%' THEN 1/0 ELSE 1 END = 1;
 secret | tenant
--------+--------
 42     | alice
```

Prefix-search that and any hidden value comes out bit by bit, whatever its type,
whether or not the type ever embeds its input in a message. Exclusion composes
with it: adding `secret NOT IN (<values already recovered>)` moves the probe to
the next hidden row, so the attack enumerates rather than samples.

So sanitizing messages changes the cost of extraction from one query per value
to a logarithmic number of queries per value. That is worth something
operationally, since a drain becomes visible to rate limiting and to audit logs
in a way a single query is not, and it is worth doing on its own merits. It is
not a fix, and it carries a real cost of its own: an error that names the value
that failed is genuinely useful for debugging, and PostgreSQL prints it for the
same reason.

The general form of the argument is why this design constrains ordering rather
than output. Any fallible expression is an oracle over whatever rows it is
allowed to observe, so the only place to intervene is which rows it observes.

**Reject fallible expressions in predicates over any view.** The sound version
of the previous entry: if an oracle needs a fallible expression, refuse the
expression. It needs no optimizer changes and it does close the channel. It also
breaks a large amount of ordinary SQL, since `::int` casts and division appear in
predicates constantly, and it would reject them over every view rather than only
the ones acting as an access boundary. It also leaves the ordering problem intact
for any future channel that is not an error.

## Open questions

- Do we want to support view-based row-level access control at all? The whole
  design is contingent on that. If yes, this is table stakes and the current
  behavior is a security bug. If no, the honest move is to document that views
  are not a security boundary and close this out.
- Is the residual risk acceptable? This proposal trades a structural guarantee
  for a rule-based one in exchange for performance and for a path to row-level
  security. The obligation on future code is scoped in
  [Residual risk](#residual-risk), and one of its three shapes is closable as a
  `Typecheck` invariant. If the team would rather have the structural guarantee
  and pay for it, the alternative is prototyped and ready.
- Should the `Typecheck` invariant land with this, or after? It converts the
  movement half of the residual risk from review discipline into a test failure,
  and it is the single highest-value follow-up.
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
