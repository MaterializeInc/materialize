# Security barrier views

- Associated: [#38562](https://github.com/MaterializeInc/materialize/pull/38562) (prototype)

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
- Making the barrier the default for all views.

## Solution Proposal

Adopt PostgreSQL's pre-9.5 model: mark the view, then decline to dissolve its
boundary. Do not adopt PostgreSQL's later `RestrictInfo.security_level`
machinery.

That machinery exists so that quals originating at many different security
levels can be interleaved at a single scan node, which is what row-level
security requires. Materialize has no RLS, so every barrier is a whole-object
boundary and a single bit per object suffices.

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

### Optimizer changes

The barrier set is optimizer-only state, so it belongs on `TransformCtx`, the
struct that already threads arguments through all transforms, rather than on
`DataflowDescription`, which is part of the compute protocol.

`DataflowBuilder` learns which imported views are barriers as it imports them,
because `import_into_dataflow` already matches on the catalog entry. Callers
hand that set to `TransformCtx::global`.

Two gates then implement the barrier:

- `inline_views` skips a barrier view, so it stays a distinct
  `objects_to_build` entry referenced through a global `Get`. Every transform
  that runs per-object then treats it as opaque for free, and no `Let` binding
  is ever formed for `push_into_let_binding` to push into.
- `optimize_dataflow_filters_inner` applies only leakproof predicates to a
  barrier view. Non-leakproof predicates stay above the `Get` in the consumer,
  and because they never enter the view's plan they also never propagate onward
  to the view's own inputs.

No other cross-object transform needs a gate. `optimize_dataflow_demand`
prunes columns rather than rows, which is safe and which PostgreSQL also
permits. Everything else in the pipeline operates within a single object.

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

Implemented in [#38562](https://github.com/MaterializeInc/materialize/pull/38562),
roughly 230 lines across 28 files, most of it option plumbing. The optimizer
change proper is two gates and one predicate in `src/transform/src/dataflow.rs`.

- `src/transform/tests/test_security_barrier.rs` asserts all three behaviors
  above at the `optimize_dataflow_filters_inner` seam, including a test that
  pins the current exposure so a regression is visible.
- `test/sqllogictest/security_barrier.slt` covers the SQL surface and the
  `EXPLAIN` output quoted above.
- The existing `EXPLAIN` and privilege suites pass unchanged (1530 assertions),
  confirming that the default path is untouched.

Deliberately left out of the prototype:

- An `mz_views` catalog column reporting the flag. `SHOW CREATE VIEW` already
  reflects it through `create_sql`.
- `ALTER VIEW ... SET (SECURITY BARRIER)`.
- Documentation under `doc/user/`.

## Alternatives

**Per-predicate security levels, as in PostgreSQL 9.5 and later.** Carrying a
level on each predicate would let barrier views be inlined and still preserve
ordering, recovering the fusion the object-level barrier gives up. It also
generalizes to row-level security. It is a far larger change: `Filter`'s
`predicates` field, `MapFilterProject`, and every transform that constructs or
reorders predicates would have to carry and respect the level, and any transform
that forgets silently produces an insecure plan. The object-level barrier gets
the same guarantee from two gates and can be upgraded later if RLS arrives.

**Make every view a barrier.** Correct by default and immune to a user
forgetting the option, but it would remove cross-view predicate pushdown from
every workload in order to protect the small fraction of views used for access
control. Not viable.

**Document that views are not a security boundary.** Legitimate, and strictly
cheaper. It is the right answer if we conclude that view-based row filtering is
not a pattern we intend to support. It conflicts with what the RBAC
documentation currently implies, so if we choose it we should say so
explicitly rather than by omission.

**Reject fallible expressions in predicates over any view.** Blocks the error
channel without any optimizer changes, but it breaks a large amount of
legitimate SQL (`::int` casts, division) and still leaves the ordering problem
for any future non-error channel.

## Open questions

- Do we want to support view-based row-level access control at all? The whole
  design is contingent on that. If yes, this is table stakes and the current
  behavior is a security bug. If no, the honest move is to document that views
  are not a security boundary and close this out.
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
