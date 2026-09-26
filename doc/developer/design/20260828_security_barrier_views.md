# Security barrier views

- Associated: [#38562](https://github.com/MaterializeInc/materialize/pull/38562)
  (object barrier prototype),
  [#38568](https://github.com/MaterializeInc/materialize/pull/38568)
  (security levels, groundwork for row-level security)

This document covers a staged plan for making a view or table a real row-level
access boundary. The security barrier is the optimizer piece of that plan, but
it is not the whole plan and not the first thing to ship. The organizing idea is
that the boundary can live at more than one layer, and the cheapest robust layer
that fits a given case is the one to use. Materialization plus access routing is
that layer wherever an object can be materialized; the optimizer barrier is the
fallback for the objects that cannot.

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

The optimizer half does not, and there are two distinct channels through which
it fails. Both let a reader observe rows the view's own filter should have
excluded.

### Channel one: reader-induced errors

A predicate supplied by the reader is pushed across the view boundary and
evaluated *before* the view's own filter, so a reader can aim a fallible
expression at rows they are not allowed to see and read the hidden values back
out of the resulting error message.

Modeling the situation directly against `optimize_dataflow_filters_inner`, with
`#0` a secret column and `#1` the tenant column:

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

### Channel two: error-stream bubbling

A Materialize collection carries an `oks` stream and an `errs` stream, and a
peek returns the error whenever `errs` is non-empty. An error produced anywhere
below a view, a source row that failed to decode, a cast that failed in the
view's own body, does not get filtered out by a downstream predicate. It
propagates up and poisons every read of the collection, so the object is
effectively in a failed state for the reader.

This is a second, independent exposure. The reader does not induce it; the data
already errored. A single malformed record demonstrates it, with the tenant
living inside the record so the filter cannot be applied without first parsing:

```sql
CREATE TABLE docs (doc text);
INSERT INTO docs VALUES
  ('{"tenant":"alice","secret":"ALICE-OK"}'),
  ('BOB-MALFORMED-PRIVATE-PAYLOAD');

CREATE VIEW parsed AS
  SELECT (doc::jsonb)->>'tenant' AS tenant, (doc::jsonb)->>'secret' AS secret FROM docs;
CREATE VIEW alice_docs AS SELECT * FROM parsed WHERE tenant = 'alice';

SELECT * FROM alice_docs;
ERROR:  invalid input syntax for type jsonb: ... "BOB-MALFORMED-PRIVATE-PAYLOAD"
```

The physical plan shows why no ordering helps: the parse feeds the filter, so it
must run first.

```
Source materialize.public.docs
  filter=(("alice" = (#1 ->> "tenant")))
  map=(text_to_jsonb(#0{doc}), (#1 ->> "secret"), "alice")
```

Three harms follow, in increasing severity: the reader learns a foreign row
exists, the error text echoes the foreign row's content verbatim, and one bad
record denies service to every reader of the view. An object barrier that only
constrains predicate placement does nothing about this channel, because the
error is generated below the barrier by the view's own definition, not by the
reader's query. This channel needs the error firewall described below.

### Why materializing does not, by itself, close either channel

`ExprPrepMaintained` rejects unmaterializable functions, so a view whose
predicate references `current_user` cannot be indexed or materialized at all.
The canonical per-tenant view is therefore always inlined into the reader's
dataflow. And even where a view can be materialized, the materialized object is
protective only when the reader is actually made to read it: an index helps only
on the cluster that holds it, and a reader chooses their own cluster. The error
channel is worse still, because the error propagates into the materialized
object's own `errs` stream, so materializing does not contain it.

## Success Criteria

1. A view or table can be marked such that no reader-supplied expression is
   evaluated against a row the object's own filter would have excluded.
2. An error produced below the boundary does not surface to a reader who could
   not see the row that produced it, and does not deny the reader service.
3. The guarantee holds against the optimizer, not merely against the plans it
   happens to produce today. A new transform that crosses the boundary should
   have to opt in rather than silently defeat it.
4. Predicates that cannot leak still cross the boundary, so the common selective
   filters keep reaching persist pushdown and index lookups.
5. The default behavior of existing objects does not change.

## Out of Scope

- `security_invoker` views. Materialize's privilege model is already
  definer-style and this design does not change it.
- Timing and introspection side channels. A boundary object's base scan still
  runs on the reader's cluster, and `mz_introspection` is `PUBLIC_SELECT`, so
  operator-level record counts and query latency remain observable. PostgreSQL
  has the same class of hole and does not address it either. See
  [Open questions](#open-questions).
- Making any of these behaviors the default. Every mechanism here is opt-in; see
  [Why the option is opt-in](#why-the-option-is-opt-in).

## Solution Proposal

The boundary can live at three layers. They are ordered here cheapest and most
robust first, and a given object should be protected at the earliest layer that
fits it.

| Layer | Mechanism | Fits |
| --- | --- | --- |
| Access + materialization | route reads through a pre-filtered arrangement (`GRANT ... THROUGH <index>`) | objects that can be materialized: per-role views on literals, and the base of an RLS policy |
| Optimizer | the object security barrier plus the error firewall | objects that cannot be materialized, above all `current_user` views |
| Ordering | security levels | the single-relation case, which is what RLS needs and what recovers the fast path for a single-relation barrier body |

The insight that makes the first layer the preferred one is that
**materialization collapses a multi-relation object into a single-relation
arrangement, which is exactly the regime where ordering is provably safe.** The
hard cases in this whole area, the ones where a fallible predicate can be
scheduled ahead of a security filter, all arise from a *join*: the security
predicate sits on one relation and the exposed value on another, and nothing
orders across the join. A materialized object has no join left. It is one
collected relation, so the security predicate and any leaky predicate share its
support and the ordering is trivially correct. Routing a reader through that
arrangement is therefore both the cheapest boundary (it reuses arrangements,
indexes, and import resolution that already exist) and the one with the fewest
ways to be wrong.

### The staged plan

1. **Document the current behavior and how materialization is protective.** No
   code. State plainly that a view is not a security boundary today, that a
   materialized view or indexed view confines errors to the reader's own rows
   when read through its arrangement, and the two caveats that limit that: a
   `current_user` view cannot be materialized, and an index protects only on its
   own cluster.

2. **`GRANT SELECT ON <object> THROUGH <index> FOR <role>`.** An access-control
   grant that binds a role's read of an object to a specific arrangement, so the
   reader can never fall into the inlined path, and cannot run an expensive
   un-indexed query from the wrong cluster. This is the access-and-materialization
   layer, and it fits the static per-role view pattern (`alice_orders`,
   `bob_orders`, each filtering on a literal).

3. **The object security barrier, plus the error firewall.** The optimizer
   mechanism, for objects that cannot be materialized. The barrier closes channel
   one by not inlining and by pushing only leakproof predicates across; the
   firewall closes channel two by not propagating errors across the boundary.
   Prototyped for the barrier at
   [#38562](https://github.com/MaterializeInc/materialize/pull/38562).

4. **Row-level security.** A policy on a table, enforced by routing reads through
   a materialized form of the table and injecting the policy predicate behind a
   barrier so no later predicate can precede it. This reuses layers 2 and 3.
   Security levels are the ordering mechanism underneath it, sound because a
   policy is single-relation. Prototyped for levels at
   [#38568](https://github.com/MaterializeInc/materialize/pull/38568).

### Where the optimizer barrier is still needed

Layer 1 requires an object that can be materialized. A view filtering on
`current_user` cannot be, so there is no arrangement to route through, and the
static per-role decomposition (`alice_orders`, `bob_orders`, ...) is not always
practical. For those objects the optimizer barrier is the only boundary, which
is why it remains a first-class part of the plan rather than a legacy fallback.
The rest of this document specifies the optimizer barrier and the error firewall
in full, then the `THROUGH <index>` grant and the RLS shape that build on
materialization.

## The object barrier

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

It is also the trust root of the whole design, and it is currently maintained
for a purpose whose incentives point the wrong way. `could_error` exists to
keep persist filter pushdown correct, where marking a function infallible makes
queries faster and a mistake is a performance bug. Under a barrier the same
mistake is a vulnerability: a function classified infallible that has any
value-dependent failure mode is an oracle through the boundary. And
`could_error` measures only whether the function's own signature returns a
`Result`. It says nothing about panics, row or string size limits hit
downstream, or memory exhaustion. Two things are therefore required before this
ships, not merely open for discussion: a test that pins the set of infallible
functions, so that a change to the classification is a deliberate and reviewed
act, and a review pass over that set for failure paths that are not a `Result`.

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

Two gates, both in `optimize_dataflow`, and nothing else in the pipeline needs
to know the concept exists:

- **No inlining.** `inline_views` skips a barrier view, so it stays a distinct
  `objects_to_build` entry referenced through a global `Get`. Every per-object
  transform already treats a global `Get` as opaque.
- **Leakproof-only pushdown.** `optimize_dataflow_filters_inner` applies only
  leakproof predicates to a barrier object.

These are the same pair PostgreSQL uses, `is_simple_subquery` and
`qual_is_pushdown_safe`, arrived at independently.

The guarantee is structural rather than by rule. A transform cannot reach
across an object boundary because reaching across is not an operation a
transform has, so a future transform that forgets about barriers loses an
optimization rather than the guarantee. Nothing reaches LIR or the compute
protocol, and no expression type changes.

### The error firewall

The barrier described above closes channel one only. Channel two, an error
generated below the barrier by the object's own definition, still propagates
through the boundary, because the barrier constrains predicate placement, not
the `errs` stream. The barrier is nonetheless the right place to close channel
two, because it is already a distinct object read through a global `Get`, which
is a clean seam at which to divert the error stream.

The rule for the first version is deliberately simple: **an error produced below
a barrier does not surface above it, regardless of whether the reader could have
seen the row that produced it.** Mechanically, the barrier object's output keeps
its `oks` stream and diverts its `errs` stream instead of forwarding it. One
operator at the object boundary, no attribution of errors to rows.

This makes a barrier view **best-effort**: a reader sees the rows that both pass
the filter and computed cleanly, and any row that errored anywhere below the
barrier is simply absent. Two consequences must be handled, and neither is
optional even in the first version:

- **Divert, do not discard.** Suppressed errors must land in a privileged
  channel, the object owner or an `mz_internal` relation, not the void.
  Otherwise the malformed row is invisible to everyone, never gets fixed, and
  silently corrupts results forever. Operators need to see that rows are being
  dropped and why; readers do not.
- **Document best-effort, and beware aggregates.** A filter or project barrier
  view that drops a row yields one missing row, which is tolerable. An aggregate
  over a barrier view silently excludes the dropped rows and returns a
  confidently wrong number. This is a second reason a barrier view should be a
  simple filter over its security predicate, reinforcing the guidance below.

The firewall also has a benefit beyond confidentiality: because an error today
poisons the whole collection, diverting it at the barrier is what lets the view
remain readable when a single record below it is bad. One point of failure stops
denying service to the whole view, not only stops leaking.

A tighter rule is available later and is a clean relaxation of this one: surface
an error only when it occurred on a row that provably passes the security
predicate, so a reader keeps their own errors while foreign and unattributable
errors are contained. The first version is a strict subset of that, since it
also drops the reader's own errored rows, so moving to it is not a breaking
change. There is no reason to build it now.

### What this fixes

Given

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

The barrier view keeps it above a separate object, and only the tenant filter
reaches `orders`:

```
Explained Query:
  Filter ((#2{amount} / (char_length(#1{secret}) - 3)) > 0)
    ReadGlobalFromSameDataflow materialize.public.barrier_orders

materialize.public.barrier_orders:
  Filter (#0{tenant} = "alice")
    ReadStorage materialize.public.orders

Source materialize.public.orders
  filter=((#0{tenant} = "alice"))
```

A leakproof predicate still crosses and still reaches the source import, where
persist pruning can use it:

```
EXPLAIN OPTIMIZED PLAN FOR SELECT * FROM barrier_orders WHERE amount = 42;

Explained Query:
  Filter (#2{amount} = 42)
    ReadGlobalFromSameDataflow materialize.public.barrier_orders

materialize.public.barrier_orders:
  Filter (#0{tenant} = "alice") AND (#2{amount} = 42)
    ReadStorage materialize.public.orders

Source materialize.public.orders
  filter=((#0{tenant} = "alice") AND (#2{amount} = 42))
```

### Why levels alone are not sufficient for views

A level constrains ordering inside one `MapFilterProject`, which is where
`sort_predicates` runs. A join is not one `MapFilterProject`. It is a chain of
closures whose order is fixed by *when each predicate's columns become
available*, and nothing sorts across closures.

Put the security filter and the reader's predicate on different relations of a
join over indexed inputs, so the optimizer chooses a delta join:

```sql
CREATE VIEW d3 WITH (SECURITY BARRIER) AS
  SELECT t1.id, t1.secret FROM t1
  JOIN t2 ON t1.id = t2.id JOIN t3 ON t1.id = t3.id
  WHERE t2.tenant = 'alice';

SELECT * FROM d3 WHERE secret::int > 0;
```

With levels and inlining, the cast needs only `t1`, so it lands in the initial
closure. The tenant filter needs `t2`, so it waits for that lookup:

```
initial_closure
  filter=((#0{id}) IS NOT NULL AND (text_to_integer(#1{secret}) > 0))   <- runs first
delta_stage[0]
  closure
    filter=((#2{tenant} = "alice"))                                     <- runs later

ERROR:  invalid input syntax for type integer: ... "HIDDEN3"
```

The MIR is correct and carries the right levels. The ordering is lost in
physical lowering, where `JoinImplementation` places each predicate in the
earliest stage whose support is available and does not consult levels.

With an object barrier the reader's predicate never enters the join at all:

```
Explained Query:
  Get::Collection materialize.public.d3
    filter=((text_to_integer(#1{secret}) > 0))     <- outside
materialize.public.d3:
  Join::Delta
    delta_stage[0] closure filter=((#2{tenant} = "alice"))
    initial_closure filter=((#0{id}) IS NOT NULL)
```

This is not a defect in the levels prototype. It is the gap PostgreSQL names in
`src/backend/optimizer/README`, as the reason levels are sound there and as the
work that would be required to flatten barrier views:

> Currently there is no need to consider that since security-prioritized quals
> can only be single-table restriction quals ... and security-barrier view
> subqueries are never flattened into the parent query. ... With extra rules for
> safe handling of security levels among join quals, it should be possible to
> let security-barrier views be flattened into the parent query, allowing more
> flexibility of planning while still preserving required ordering of qual
> evaluation. But that will come later.

That text has been on master since January 2017. The single-relation case is
different, and it is validated: a single-relation barrier body forced through a
delta join stays safe, because both predicates share the one relation's support
and land in the same closure where the level sort orders them. This is the
foundation of both RLS and the fast-path recovery below.

## Access and materialization

### `GRANT SELECT ... THROUGH <index>`

An indexed or materialized view of a filtered object holds only the filtered
rows, so a reader who is *made* to read it can only ever evaluate a fallible
expression against rows they are allowed to see. The proposed grant makes that
routing part of the access model:

```sql
CREATE VIEW alice_orders AS SELECT * FROM orders WHERE tenant = 'alice';
CREATE INDEX alice_orders_idx ON alice_orders (id);
GRANT SELECT ON alice_orders THROUGH alice_orders_idx TO alice;
```

The grant binds alice's read of `alice_orders` to the arrangement. She cannot
fall into the inlined path that produces the leak, and she cannot run an
expensive un-indexed query from a cluster that does not hold the index. It buys
two things at once, security and a performance bound, and it reuses machinery
that already exists rather than changing the optimizer.

This is validated. Reading a filtered indexed view through its arrangement
confines errors to the reader's own rows:

```
-- read on the cluster holding the index: the cast only ever sees alice's rows
Explained Query (fast path):
  Filter (text_to_integer(#0{secret}) > 0)
    ReadIndex on=materialize.public.alice_orders avi=[*** full scan ***]
-- errors, if any, are on alice's own data

-- read on a cluster WITHOUT the index: the view is inlined and the cast
-- reaches the base collection, erroring on a foreign row
Source materialize.public.orders
  filter=((text_to_integer(#0{secret}) > 0) AND (#1{tenant} = "alice"))
```

The off-cluster read is exactly the hole the grant closes: without it, "index
the view" is not a boundary because the reader picks the cluster. Note that the
protective property is that errors are *confined to the reader's own rows*, not
that errors disappear; a reader's own bad row still errors, which is where the
error firewall complements this layer.

The scope limit to state plainly: this fits objects that can be materialized,
which is the static per-role pattern (`alice_orders`, `bob_orders`, each
filtering on a literal). It does not fit a single `WHERE tenant = current_user`
view, which cannot be materialized at all. That case needs the optimizer
barrier, or RLS.

Two design questions remain open, both in [Open questions](#open-questions):
what happens when a granted reader writes a query the arrangement cannot serve,
and whether the grant names a specific index or any index on the object.

### Row-level security

A policy on a table has the same shape as a barrier view, with two differences:
the predicate is injected automatically at read time rather than written into a
view body, and it is parameterized by the reader's identity.

```sql
CREATE POLICY tenant_isolation ON orders USING (tenant = current_user);
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
GRANT SELECT ON orders TO alice;
```

The proposed enforcement is to route reads through a materialized form of the
table and inject the policy predicate behind a barrier, so no later predicate
can precede it. This is layers 2 and 3 applied to a table, and it is easier than
the general view case for a concrete reason: **materialization collapses the
object into a single relation, which is the regime where ordering is provably
safe.** A view kept live can be a join, which is where the delta-join gap lives;
a materialized arrangement has no join left, so the policy predicate and any
reader predicate share the arrangement's support and order correctly.

The `current_user` wall is handled by materializing the *unfiltered* base, which
is materializable, rather than the per-user filter, which is not. The
arrangement then holds all tenants' rows, and the policy predicate is injected
at read against it, ordered first by a barrier or a security level. The cost is
the standard RLS profile: one shared arrangement holding every tenant's data,
with the policy applied per query. The policy predicate still needs the ordering
mechanism, since position alone could otherwise run a reader's cast first, but
it now operates in the safe single-relation regime.

Security levels are the ordering mechanism this rests on. They are sound for a
policy because a policy predicate references only its own table, so its support
is a subset of any predicate that could leak that table's data, and it therefore
lands no later in evaluation. This is the property PostgreSQL relies on:
"security-prioritized quals can only be single-table restriction quals". The
single-relation validation in
[Why levels alone are not sufficient for views](#why-levels-alone-are-not-sufficient-for-views)
is the evidence for this path.

No RLS syntax or catalog design is specified here; this section fixes only the
enforcement shape and why it is sound.

### Recovering the fast path for single-relation barrier views

The same single-relation property gives an optional optimization for barrier
views. A barrier view whose body is a single relation (`Filter`/`Map`/`Project`
over one `Get`) can be inlined and ordered with a level rather than fenced as a
separate object, which recovers the fast-path peek that the object barrier gives
up (see [Fast-path peeks](#fast-path-peeks)). It is sound for the same reason
RLS is: one relation means the security predicate and any leaky predicate share
support and land in the same closure. The classifier must be conservative,
falling back to the object barrier for anything that is not provably a single
`Get`, so unions, subqueries, aggregates, and joins are all fenced. This is a
pure optimization with no change in semantics, so it can land whenever the
fast-path cost justifies it.

## Costs

The costs below are the price of the object barrier, and are measured. Layer 1
avoids them where it applies, which is part of why it is preferred.

### Fast-path peeks

A barrier view no longer qualifies for the fast-path peek, because it occupies
its own `objects_to_build` entry:

```
-- plain view, point lookup
Explained Query (fast path):
  Filter (#1{tenant} = "alice")
    ReadIndex on=t2 t2i=[lookup value=(1)]

-- barrier view, same query
Explained Query:                              <- no fast path, builds a dataflow
  Get::Collection materialize.public.secured
```

The index lookup itself survives, because `literal_constraints` pushes the
literal into the view and a literal is leakproof, so this is a dataflow doing a
point lookup rather than a full scan. It is still a dataflow install instead of
a peek, which is the difference between sub-millisecond and tens of
milliseconds. For a per-tenant serving workload that is the wrong direction, and
it is the case that both `THROUGH <index>` and the single-relation fast-path
recovery are meant to serve.

### Join-order fence

A barrier prevents join reordering with anything outside it. Joining a barrier
view with a second view, where the second view is small and selective:

| | barrier | no barrier |
| --- | --- | --- |
| shape | sealed inner join, then a linear join | one wider delta join |
| outer index | `(*** full scan ***)` | `delta join lookup` |

The selectivity of the outer relation can no longer prune the inner join. This
is the same effect PostgreSQL pays, where a barrier turns a merge join that
drives from the selective relation into a hash join over the full inner result.

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
barrier the cast stays above the view and leaves the source filter:

```
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
cheap because it is wrong.

**It is not a scan-volume cost.** Persist filter pushdown already treats a
fallible expression conservatively, because it must not discard a part whose
interior rows would error, so such a predicate was never driving much part
pruning. What a barrier gives up is early filtering ahead of arrangements and
joins, which is a narrower and better-defined thing to reason about.

## Why the option is opt-in

Every mechanism here defaults off.

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

**The cost is measurable and broad under default-on.** Applying barriers to
every user view moves 327 lines of the `EXPLAIN` corpus, and running the same
experiment with the predicate gate disabled produces a byte-identical diff, so
all of that cost comes from declining to inline rather than from the security
property. One case regresses from `Constant <empty>` to a full differential join
with two arrangements over a base collection, to compute a provably empty result.

**The marker means something beyond the optimizer.** It records which views are
access-control boundaries, which is information a reader of the schema wants and
which no amount of default-on behaviour supplies. It also scopes what we have to
defend: "declared views enforce ordering" is a claim we can state and test,
where "no view ever evaluates a reader's fallible predicate early" is a
whole-system property about every view, forever.

**PostgreSQL has kept it opt-in since 9.2**, and its stated reason is the same
category of cost.

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
barriers. That closes the asymmetry at a fraction of the cost of default-on.

## When to enable a barrier

**Enable it when the view is the access boundary**: the reader holds `SELECT`
on the view and not on what it reads, and the rows it filters out are rows the
reader must not learn about. That is the only case the feature is for. A view
that merely tidies up a query the reader could have written themselves does not
need one.

**Prefer materialization and routing where it fits.** If the object can be
materialized and read through an arrangement, `THROUGH <index>` is cheaper and
avoids the barrier's costs entirely. The barrier is for the objects that cannot
take that path, above all `current_user` views.

**The barrier is expensive when** the view is read by point lookups, per
[Fast-path peeks](#fast-path-peeks), or when a reader's fallible predicate is
selective and sits above a join or an arrangement.

**Materializing alone is not a substitute for the barrier** unless access is
routed through the arrangement. An index only helps on the cluster that holds
it, and a reader chooses their own cluster, so an attacker runs the query
somewhere the index is absent and the view is inlined again; that is what
`THROUGH <index>` fixes. A materialized view that is a replacement target is
imported from its view definition rather than its shard, and is inlined. And a
view whose predicate calls `current_user` can be neither indexed nor
materialized, because `ExprPrepMaintained` rejects unmaterializable functions.

### Guidance for barrier view authors

The barrier orders the reader's predicates relative to the view's. It does not
order the view's own predicates relative to each other. Inside the barrier,
predicates are still scheduled by column position, so a fallible expression the
author writes alongside the security predicate can run first, over every
tenant's rows:

```sql
CREATE VIEW c WITH (SECURITY BARRIER) AS
  SELECT * FROM base WHERE tenant = 'alice' AND secret::int > 0;

SELECT * FROM c;
ERROR:  invalid input syntax for type integer: ... "HIDDEN"    -- another tenant's row
```

```
Source materialize.public.base
  filter=((text_to_integer(#1{secret}) > 0) AND (#2{tenant} = "alice"))
```

This is not a property of the barrier. A materialized view or an indexed view
with the same body fails the same way, with the same error, to every reader.
It is a property of any object that both filters for security and evaluates a
fallible expression, and the barrier neither causes nor cures it.

**A barrier view should contain only its security predicate.** Everything else
belongs in a plain view layered above it, where it sits above the seam and is
evaluated only against rows the barrier admitted:

```sql
CREATE VIEW c_sec WITH (SECURITY BARRIER) AS SELECT * FROM base WHERE tenant = 'alice';
CREATE VIEW c_biz AS SELECT * FROM c_sec WHERE secret::int > 0;
```

Because the marker identifies the object as a boundary, `CREATE VIEW ... WITH
(SECURITY BARRIER)` should raise a `NOTICE` when the body contains a fallible
predicate that references a column. That warning is only possible for barrier
views. A general version would fire on every materialized view with a cast in
it, which is one more thing the marker buys beyond the optimizer.

## Minimal Viable Prototype

[#38562](https://github.com/MaterializeInc/materialize/pull/38562) implements
the object barrier's two gates and is complete. Validated against the CI image
for its own head across twelve query shapes, all safe: the three-way delta join
above, a plain view wrapping a barrier, a plain view joining a barrier, two
levels of wrapping, a barrier self-join, a derived table, a CTE, a materialized
view over a barrier, two barrier views joined, exclusion chaining with
`NOT IN`, and a fallible cast in the join condition.

[#38568](https://github.com/MaterializeInc/materialize/pull/38568) implements
security levels and is the basis for RLS and the single-relation fast-path
recovery. The single-relation-through-a-delta-join case is validated safe on its
CI image, distinguished from the multi-relation leak by a discriminating column
order so that the level, not position, is shown to do the ordering.

Not yet built, and required before the object barrier ships:

- The error firewall, per [The error firewall](#the-error-firewall).
- The `NOTICE` for a fallible predicate in a barrier body, per
  [Guidance for barrier view authors](#guidance-for-barrier-view-authors).
- The test pinning the infallible function set, per
  [Leakproofness](#leakproofness).

Not yet built, and belonging to later stages:

- The `THROUGH <index>` grant.
- RLS syntax, catalog, and policy injection.
- The single-relation fast-path classifier.

Deliberately left out of the barrier prototype: an `mz_views` catalog column
reporting the flag (`SHOW CREATE VIEW` already reflects it through `create_sql`),
`ALTER VIEW ... SET (SECURITY BARRIER)`, and user documentation under
`doc/user/`.

## Alternatives

**Security levels instead of the barrier, for views.** Carry a level on each
predicate and constrain movement and evaluation order by level, inlining the
view as usual. Prototyped and formally verified at
[#38568](https://github.com/MaterializeInc/materialize/pull/38568): 1768
sqllogictest assertions pass, no text plan in the `EXPLAIN` corpus changes, and
the ordering rule is stated declaratively in
`src/expr/tests/test_security_levels.rs` and verified red before green.

Rejected **as the general mechanism for views**, because a view body can be a
join and levels do not survive one; see
[Why levels alone are not sufficient for views](#why-levels-alone-are-not-sufficient-for-views).
It is not rejected as a mechanism: it is the ordering layer that RLS and the
single-relation fast-path recovery rest on. Its costs, should the full
inlined-with-levels path ship, are that `MapFilterProject` carries the level
into LIR and the compute protocol, `Predicate` is 104 bytes against
`MirScalarExpr`'s 96, and the serialized MIR nests `{expr, level}`, which
changes `EXPLAIN AS JSON` and requires an expression-cache format bump.

**Make every view a barrier.** Correct by default and immune to a user
forgetting the option. Rejected for the reasons in
[Why the option is opt-in](#why-the-option-is-opt-in).

**Wrap the predicate in an opaque marker.** Another way to keep the view
inlined while constraining movement, so a sibling of levels rather than of the
barrier: a `SecurityFence` unary function that the optimizer refuses to move.
Prototyped: it keeps the view inlined and needs only 7 files, because an
unrecognized wrapper is inert by default rather than requiring every transform
to opt in. It was abandoned because wrapping makes a predicate syntactically
unrecognizable, which is fail-safe where a transform is optional but fail-stop
where its output is required. Temporal filters break outright: `mz_now()` is
non-leakproof, so it gets wrapped, and `MfpPlan` then rejects it as an
unsupported temporal predicate.

**Document that views are not a security boundary.** Legitimate, and strictly
cheaper than any mechanism. It is the right answer if we conclude that
view-based row filtering is not a pattern we intend to support. It conflicts
with what the RBAC documentation currently implies, so if we choose it we should
say so explicitly rather than by omission. Note that stage one of the plan does
part of this honestly regardless, by documenting the current behavior.

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

The general form of the argument is why this design constrains placement and
ordering rather than output. Any fallible expression is an oracle over whatever
rows it is allowed to observe, so the only place to intervene is which rows it
observes.

**Reject fallible expressions in predicates over any view.** The sound version
of the previous entry: if an oracle needs a fallible expression, refuse the
expression. It needs no optimizer changes and it does close the channel. It also
breaks a large amount of ordinary SQL, since `::int` casts and division appear in
predicates constantly, and it would reject them over every view rather than only
the ones acting as an access boundary. It also leaves the problem intact for any
future channel that is not an error.

## Open questions

- Do we want to support view- and row-level access control at all? The whole
  plan is contingent on that. If yes, this is table stakes and the current
  behavior is a security bug. If no, the honest move is stage one alone,
  documenting that views are not a security boundary, and closing the rest out.
- `THROUGH <index>` enforcement: what happens when a granted reader writes a
  query the arrangement cannot serve? A non-key predicate becomes a full scan of
  the arrangement, which is still safe and on-cluster, so that is the natural
  floor; only a query that would need to leave the arrangement should be
  refused. And does the grant name a specific index or any index on the object?
  Any index on a filtered view holds only the filtered rows, so any is safe;
  naming one is about predictable cost.
- Error firewall semantics: the first version drops the reader's own errored
  rows along with foreign ones. Is that acceptable as a starting point, with the
  tighter "surface errors on rows that provably pass" rule as a later
  relaxation? Where should diverted errors surface for operators?
- `MirScalarExpr::could_error` becomes a security boundary under the barrier, so
  a wrong `could_error = false` is a vulnerability rather than a performance bug.
  The pinning test and the audit in [Leakproofness](#leakproofness) are required;
  the open question is whether an explicit policy beyond those is warranted.
- Timing and `mz_introspection` remain oracles for the cardinality of the
  pre-filter scan, since a barrier view still runs on the reader's cluster. Is
  that acceptable, as it is in PostgreSQL, or does a serious answer require
  running the view on a cluster the reader does not have introspection access
  to?
- Should a barrier view be allowed to depend on a non-barrier view that reads
  the same protected table? Nothing prevents it, and it is a plausible way to
  build a barrier that does not actually protect anything.
