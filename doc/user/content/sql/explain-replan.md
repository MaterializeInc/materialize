---
title: "EXPLAIN REPLAN"
description: "Reference page for `EXPLAIN REPLAN`. `EXPLAIN REPLAN` re-runs the optimizer on an existing view, materialized view, or index and displays the resulting plan."
menu:
  main:
    parent: commands
---

`EXPLAIN REPLAN` re-runs the optimizer on an existing view, materialized view,
or index, and displays the resulting plan. Plain
[`EXPLAIN PLAN`](/sql/explain-plan/) for one of those objects prints the plan
that is already installed. `EXPLAIN REPLAN` instead re-parses and re-optimizes
the `CREATE` statement that Materialize stored for the object, which shows how
the object would be planned the next time Materialize plans it from scratch, for
example under a different optimizer feature setting.

`REPLAN` is an [explained object](/sql/explain-plan/#explained-object) of
`EXPLAIN PLAN` rather than a statement in its own right, so the stages, output
formats, and output modifiers are those of
[`EXPLAIN PLAN`](/sql/explain-plan/).

{{< warning >}}
`EXPLAIN` is not part of Materialize's stable interface and is not subject to
our backwards compatibility guarantee. The syntax and output of `EXPLAIN` may
change arbitrarily in future versions of Materialize.
{{< /warning >}}

## Syntax

{{% include-syntax file="examples/explain_replan" example="syntax" %}}

## Details

`EXPLAIN REPLAN` reads the `CREATE` statement that Materialize stored for the
object, re-parses it, and pushes it through the same optimizer pipeline that the
original `CREATE` statement went through. Two details distinguish that from
optimizing the statement again from scratch:

* **Indexes created after the object are hidden from the optimizer.**
  Materialize assigns catalog objects internal identifiers in creation order,
  and in replan mode the optimizer ignores every index whose identifier is
  greater than or equal to the replanned object's own identifier. This
  reproduces the catalog state that the optimizer sees when it re-plans the
  object during startup, where an index cannot be used by an object that was
  created before it.

* **The target cluster comes from the object, not from the session.** The
  cluster is taken from the object's own `IN CLUSTER` clause, so any
  cluster-level optimizer features configured with
  [`ALTER CLUSTER`](/sql/alter-cluster/) apply to the replan.

`EXPLAIN REPLAN` only plans. It does not install a dataflow, modify the object,
or affect the plan that is currently running.

### Comparison with the other `EXPLAIN PLAN` forms

Three `EXPLAIN` forms can report a plan for the same materialized view:

| Statement | What it reports | Runs the optimizer | Sees indexes created after the object |
|-----------|-----------------|--------------------|---------------------------------------|
| `EXPLAIN ... FOR MATERIALIZED VIEW <name>` | The plan that is installed right now. | No | No |
| `EXPLAIN ... FOR REPLAN MATERIALIZED VIEW <name>` | The plan the object would get if it were planned from scratch today. | Yes | No |
| `EXPLAIN ... FOR CREATE MATERIALIZED VIEW <name> AS ...` | The plan a brand new object with that definition would get. | Yes | Yes |

Because the plain form serves a stored plan rather than optimizing anything, the
optimizer feature overrides described below have no effect on it. Because the
`CREATE` form optimizes against the current catalog, it can pick up indexes that
the existing object is not allowed to use. `EXPLAIN REPLAN` is the only form
that both runs the optimizer and restricts it to the access paths the existing
object could actually use.

### Supported stages

Not every optimization stage applies to every object type. A view is optimized
only up to the locally optimized stage, so it has no `OPTIMIZED` or `PHYSICAL`
plan of its own. An index is built from an already optimized input, so it has no
`RAW`, `DECORRELATED`, or `LOCALLY OPTIMIZED` plan.

| Stage | `REPLAN VIEW` | `REPLAN MATERIALIZED VIEW` | `REPLAN INDEX` |
|-------|---------------|----------------------------|----------------|
| Stage omitted (defaults to `PHYSICAL PLAN AS TEXT`) | No | Yes | Yes |
| `RAW PLAN` | Yes | Yes | No |
| `DECORRELATED PLAN` | Yes | Yes | No |
| `LOCALLY OPTIMIZED PLAN` | Yes | Yes | No |
| `OPTIMIZED PLAN` | No | Yes | Yes |
| `PHYSICAL PLAN` | No | Yes | Yes |

Naming `PHYSICAL PLAN` explicitly without an `AS` clause selects the more
verbose rendering, exactly as it does for the other explained objects, so
`EXPLAIN REPLAN MATERIALIZED VIEW <name>` and
`EXPLAIN PHYSICAL PLAN FOR REPLAN MATERIALIZED VIEW <name>` report the same plan
at different levels of detail.

Since no default stage is valid for a view, `REPLAN VIEW` always requires an
explicit stage:

```mzsql
EXPLAIN REPLAN VIEW large_orders;
```
```
ERROR:  EXPLAIN statement for a view needs an explicit stage
LINE 1: EXPLAIN REPLAN VIEW large_orders;
                ^
```

For a description of each stage and of the operators that appear in the output,
see [`EXPLAIN PLAN`](/sql/explain-plan/#explained-stage) and
[Explain plan operators](/reference/explain-plan-operators/).

### Optimizer feature overrides

Alongside the [output modifiers](/sql/explain-plan/#output-modifiers) that
`EXPLAIN PLAN` accepts, the `WITH` clause accepts optimizer feature overrides of
the form `ENABLE <feature> = <boolean>`. An override applies only to the
statement it appears on, and changes nothing about the object or about any
system or cluster setting.

This is the main use for `EXPLAIN REPLAN`: checking what plan an existing object
would get if a given optimizer feature were turned on or off, without recreating
the object and without changing a setting for anyone else. The set of
overridable features is internal to the optimizer and changes between versions,
so treat feature names as specific to the version you are running.

## Examples

The examples below use the following schema. Note that the index on `customers`
is created *after* the materialized view that could have used it, which is what
makes the replanned plan differ from a freshly planned one.

```mzsql
CREATE SCHEMA replan_demo;
SET search_path = replan_demo;

CREATE TABLE orders (id int, customer_id int, amount numeric);
CREATE TABLE customers (customer_id int, region text);
CREATE TABLE order_notes (id bigint, note text);

CREATE MATERIALIZED VIEW enriched_orders AS
    SELECT o.id, o.amount, c.region
    FROM orders o JOIN customers c USING (customer_id);

CREATE INDEX customers_customer_id_idx ON customers (customer_id);

CREATE VIEW large_orders AS
    SELECT id, customer_id, amount FROM orders WHERE amount > 1000;

CREATE INDEX large_orders_id_idx ON large_orders (id);

CREATE VIEW annotated_orders AS
    SELECT * FROM orders o LEFT JOIN order_notes n USING (id)
    WHERE o.amount = 100;
```

### Replanning a materialized view

With no stage specified, `EXPLAIN REPLAN` reports the physical plan:

```mzsql
EXPLAIN REPLAN MATERIALIZED VIEW enriched_orders;
```
```
                                  Physical Plan
---------------------------------------------------------------------------------
 materialize.replan_demo.enriched_orders:                                       +
   →Differential Join %0:orders[#1{customer_id}] » %1:customers[#0{customer_id}]+
     →Arrange (#1{customer_id})                                                 +
       →Read materialize.replan_demo.orders                                     +
     →Arrange (#0{customer_id})                                                 +
       →Read materialize.replan_demo.customers                                  +
                                                                                +
 Source materialize.replan_demo.orders                                          +
   filter=((#1{customer_id}) IS NOT NULL)                                       +
   pushdown=((#1{customer_id}) IS NOT NULL)                                     +
 Source materialize.replan_demo.customers                                       +
   filter=((#0{customer_id}) IS NOT NULL)                                       +
   pushdown=((#0{customer_id}) IS NOT NULL)                                     +
                                                                                +
 Target cluster: quickstart                                                     +
```

### Comparing a stored plan, a replan, and a fresh plan

The installed plan for `enriched_orders` reads both inputs from storage, because
`customers_customer_id_idx` did not exist when the materialized view was
created:

```mzsql
EXPLAIN OPTIMIZED PLAN FOR
MATERIALIZED VIEW enriched_orders;
```
```
                                  Optimized Plan
-----------------------------------------------------------------------------------
 materialize.replan_demo.enriched_orders:                                         +
   Project (#0{id}, #2{amount}, #4{region}) // { arity: 3 }                       +
     Join on=(#1{customer_id} = #3{customer_id}) type=differential // { arity: 5 }+
       ArrangeBy keys=[[#1{customer_id}]] // { arity: 3 }                         +
         Filter (#1{customer_id}) IS NOT NULL // { arity: 3 }                     +
           ReadStorage materialize.replan_demo.orders // { arity: 3 }             +
       ArrangeBy keys=[[#0{customer_id}]] // { arity: 2 }                         +
         Filter (#0{customer_id}) IS NOT NULL // { arity: 2 }                     +
           ReadStorage materialize.replan_demo.customers // { arity: 2 }          +
                                                                                  +
 Source materialize.replan_demo.orders                                            +
   filter=((#1{customer_id}) IS NOT NULL)                                         +
   pushdown=((#1{customer_id}) IS NOT NULL)                                       +
 Source materialize.replan_demo.customers                                         +
   filter=((#0{customer_id}) IS NOT NULL)                                         +
   pushdown=((#0{customer_id}) IS NOT NULL)                                       +
                                                                                  +
 Target cluster: quickstart                                                       +
```

Replanning the same object reproduces that plan exactly. The optimizer runs
again, but the index is still hidden from it, because the index was created
after the materialized view:

```mzsql
EXPLAIN OPTIMIZED PLAN FOR
REPLAN MATERIALIZED VIEW enriched_orders;
```
```
                                  Optimized Plan
-----------------------------------------------------------------------------------
 materialize.replan_demo.enriched_orders:                                         +
   Project (#0{id}, #2{amount}, #4{region}) // { arity: 3 }                       +
     Join on=(#1{customer_id} = #3{customer_id}) type=differential // { arity: 5 }+
       ArrangeBy keys=[[#1{customer_id}]] // { arity: 3 }                         +
         Filter (#1{customer_id}) IS NOT NULL // { arity: 3 }                     +
           ReadStorage materialize.replan_demo.orders // { arity: 3 }             +
       ArrangeBy keys=[[#0{customer_id}]] // { arity: 2 }                         +
         Filter (#0{customer_id}) IS NOT NULL // { arity: 2 }                     +
           ReadStorage materialize.replan_demo.customers // { arity: 2 }          +
                                                                                  +
 Source materialize.replan_demo.orders                                            +
   filter=((#1{customer_id}) IS NOT NULL)                                         +
   pushdown=((#1{customer_id}) IS NOT NULL)                                       +
 Source materialize.replan_demo.customers                                         +
   filter=((#0{customer_id}) IS NOT NULL)                                         +
   pushdown=((#0{customer_id}) IS NOT NULL)                                       +
                                                                                  +
 Target cluster: quickstart                                                       +
```

Explaining the equivalent `CREATE MATERIALIZED VIEW` statement optimizes against
the current catalog instead, so the index is available and the plan changes. The
join now reads `customers` through the index, the `customers` source disappears
from the plan, and a `Used Indexes` section appears:

```mzsql
EXPLAIN OPTIMIZED PLAN FOR
CREATE MATERIALIZED VIEW enriched_orders AS
    SELECT o.id, o.amount, c.region
    FROM orders o JOIN customers c USING (customer_id);
```
```
                                        Optimized Plan
----------------------------------------------------------------------------------------------
 materialize.replan_demo.enriched_orders:                                                    +
   Project (#0{id}, #2{amount}, #4{region}) // { arity: 3 }                                  +
     Join on=(#1{customer_id} = #3{customer_id}) type=differential // { arity: 5 }           +
       ArrangeBy keys=[[#1{customer_id}]] // { arity: 3 }                                    +
         Filter (#1{customer_id}) IS NOT NULL // { arity: 3 }                                +
           ReadStorage materialize.replan_demo.orders // { arity: 3 }                        +
       ArrangeBy keys=[[#0{customer_id}]] // { arity: 2 }                                    +
         ReadIndex on=customers customers_customer_id_idx=[differential join] // { arity: 2 }+
                                                                                             +
 Source materialize.replan_demo.orders                                                       +
   filter=((#1{customer_id}) IS NOT NULL)                                                    +
   pushdown=((#1{customer_id}) IS NOT NULL)                                                  +
                                                                                             +
 Used Indexes:                                                                               +
   - materialize.replan_demo.customers_customer_id_idx (differential join)                   +
                                                                                             +
 Target cluster: quickstart                                                                  +
```

Read the `REPLAN` output to see how `enriched_orders` will be planned after a
restart, and the `CREATE` output to see what recreating it today would give you.

### Replanning with an optimizer feature override

`annotated_orders` contains an outer join, so its locally optimized plan depends
on how the optimizer lowers outer joins. Replanning it with the default settings
gives:

```mzsql
EXPLAIN LOCALLY OPTIMIZED PLAN FOR
REPLAN VIEW annotated_orders;
```
```
                               Locally Optimized Plan
------------------------------------------------------------------------------------
 With                                                                              +
   cte l0 =                                                                        +
     Join on=(#3{id} = integer_to_bigint(#0{id})) // { arity: 5 }                  +
       Filter (#0{id}) IS NOT NULL // { arity: 3 }                                 +
         Get materialize.replan_demo.orders // { arity: 3 }                        +
       Filter (#0{id}) IS NOT NULL // { arity: 2 }                                 +
         Get materialize.replan_demo.order_notes // { arity: 2 }                   +
 Return // { arity: 4 }                                                            +
   Project (#0{id}..=#2{amount}, #4{note}) // { arity: 4 }                         +
     Union // { arity: 5 }                                                         +
       Map (null, null) // { arity: 5 }                                            +
         Union // { arity: 3 }                                                     +
           Project (#0{id}..=#2{amount}) // { arity: 3 }                           +
             Negate // { arity: 4 }                                                +
               Join on=(#3{id} = integer_to_bigint(#0{id})) // { arity: 4 }        +
                 Filter (#2{amount} = 100) AND (#0{id}) IS NOT NULL // { arity: 3 }+
                   Get materialize.replan_demo.orders // { arity: 3 }              +
                 Distinct project=[#3{id}] // { arity: 1 }                         +
                   Get l0 // { arity: 5 }                                          +
           Filter (#2{amount} = 100) // { arity: 3 }                               +
             Get materialize.replan_demo.orders // { arity: 3 }                    +
       Filter (#2{amount} = 100) // { arity: 5 }                                   +
         Get l0 // { arity: 5 }                                                    +
```

Adding an override to the same statement replans the view as if the new outer
join lowering were turned off, and yields a visibly different plan. Feature
names are specific to the Materialize version you are running:

```mzsql
EXPLAIN LOCALLY OPTIMIZED PLAN WITH (ENABLE NEW OUTER JOIN LOWERING = FALSE) FOR
REPLAN VIEW annotated_orders;
```
```
                                  Locally Optimized Plan
-------------------------------------------------------------------------------------------
 With                                                                                     +
   cte l0 =                                                                               +
     Join on=(#3{id} = integer_to_bigint(#0{id})) // { arity: 5 }                         +
       Filter (#2{amount} = 100) AND (#0{id}) IS NOT NULL // { arity: 3 }                 +
         Get materialize.replan_demo.orders // { arity: 3 }                               +
       Filter (#0{id}) IS NOT NULL // { arity: 2 }                                        +
         Get materialize.replan_demo.order_notes // { arity: 2 }                          +
   cte l1 =                                                                               +
     Filter (#2{amount} = 100) // { arity: 3 }                                            +
       Get materialize.replan_demo.orders // { arity: 3 }                                 +
 Return // { arity: 4 }                                                                   +
   Project (#0{id}..=#2{amount}, #4{note}) // { arity: 4 }                                +
     Union // { arity: 5 }                                                                +
       Get l0 // { arity: 5 }                                                             +
       Project (#0{id}, #1{customer_id}, #5..=#7) // { arity: 5 }                         +
         Map (100, null, null) // { arity: 8 }                                            +
           Join on=(#0{id} = #2{id} AND #1{customer_id} = #3{customer_id}) // { arity: 5 }+
             Union // { arity: 2 }                                                        +
               Negate // { arity: 2 }                                                     +
                 Distinct project=[#0{id}, #1{customer_id}] // { arity: 2 }               +
                   Get l0 // { arity: 5 }                                                 +
               Distinct project=[#0{id}, #1{customer_id}] // { arity: 2 }                 +
                 Get l1 // { arity: 3 }                                                   +
             Get l1 // { arity: 3 }                                                       +
```

The view itself is untouched. Only this one explanation was planned with the
feature disabled.

### Replanning an index

An index is replanned together with the view it is built on, so the output
contains one plan per object in the resulting dataflow:

```mzsql
EXPLAIN OPTIMIZED PLAN FOR
REPLAN INDEX large_orders_id_idx;
```
```
                                   Optimized Plan
-------------------------------------------------------------------------------------
 materialize.replan_demo.large_orders_id_idx:                                       +
   ArrangeBy keys=[[#0{id}]] // { arity: 3 }                                        +
     ReadGlobalFromSameDataflow materialize.replan_demo.large_orders // { arity: 3 }+
                                                                                    +
 materialize.replan_demo.large_orders:                                              +
   Filter (#2{amount} > 1000) // { arity: 3 }                                       +
     ReadStorage materialize.replan_demo.orders // { arity: 3 }                     +
                                                                                    +
 Source materialize.replan_demo.orders                                              +
   filter=((#2{amount} > 1000))                                                     +
   pushdown=((#2{amount} > 1000))                                                   +
                                                                                    +
 Target cluster: quickstart                                                         +
```

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/explain-replan" %}}
