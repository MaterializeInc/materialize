---
title: "EXPLAIN"
description: "`EXPLAIN` displays the plan used for a `SELECT` statement or a view."
menu:
  main:
    parent: commands
---

`EXPLAIN` displays the plan used for a `SELECT` statement or a view.

{{< warning >}}
`EXPLAIN` is not part of Materialize's stable interface and is not subject to
our backwards compatibility guarantee. The syntax and output of `EXPLAIN` may
change arbitrarily in future versions of Materialize.
{{< /warning >}}

## Conceptual framework

To execute `SELECT` statements, Materialize generates a plan consisting of
operators that interface with our underlying Differential dataflow engine.
`EXPLAIN` lets you see the plan for a given query, which can provide insight
into Materialize's behavior for specific queries, e.g. performance.

## Syntax

{{< diagram "explain.svg" >}}

Field | Use
------|-----
**TYPED** | Annotate the plan with column types and unique keys
**RAW** | Display the raw plan
**DECORRELATED** | Display the decorrelated plan
**OPTIMIZED** | _(Default)_ Display the optimized plan
**VIEW** | Display the plan for an existing view

## Details

The job of the Materialize planner is to turn SQL code into differential
dataflow. We get there via a series of progressively lower-level plans:

`SQL -> raw plan -> decorrelated plan -> optimized plan -> dataflow`

#### From SQL to raw plan

In this stage, the planner:

- Replaces SQL variable names with column numbers
- Infers the type of each expression
- Choose the correct overload for each function

#### From raw plan to decorrelated plan

In this stage, the planner:

- Replaces subqueries and lateral joins with non-nested operations
- Replaces `OUTER` joins with lower-level operations
- Replaces aggregate default values with lower-level operations

#### From decorrelated plan to optimized plan

In this stage, the planner:

- Coalesces joins
- Chooses join order and implementation
- Fuses adjacent operations
- Removes redundant operations
- Evaluates any operations on constants

#### From optimized plan to dataflow

In this stage, the planner:

- Maps plan operators to differential dataflow operators
- Locates existing arrangements which can be reused
- Installs the new dataflow into the running system

For more information on using `EXPLAIN`, see the [details](#details) below.

### Reading decorrelated/optimized plans

Materialize plans are directed acyclic graphs of operators. Each operator in the
graph receives inputs from zero or more other operators and produces a single
output.

Most operators have only one input, so to make the displayed plan easier to read
we group chains of single-input operators together into a single block of text
in the display. Here is a single chain of operators:

```
%0 =
| Get materialize.public.customer (u15)
| ArrangeBy (#0)
```

Multiple chains are separated by blank lines.

```
%0 =
| Get materialize.public.customer (u15)
| ArrangeBy (#0)

%1 =
| Get materialize.public.orders (u18)
| ArrangeBy (#0) (#1)
```

Each chain is assigned a number eg `%1`. Operators which have multiple inputs
refer to these numbers to specify their inputs. In the next example the `Join`
operator use the outputs of chains `%0`, `%1` and `%2` as its inputs.

```
%0 =
| Get materialize.public.customer (u15)
| ArrangeBy (#0)

%1 =
| Get materialize.public.orders (u18)
| ArrangeBy (#0) (#1)

%2 =
| Get materialize.public.lineitem (u21)
| ArrangeBy (#0)

%3 =
| Join %0 %1 %2 (= #8 #17) (= #0 #9)
| Filter (#6 = "BUILDING"), (#12 < 1995-03-15), (#27 > 1995-03-15)
| Reduce group=(#8, #12, #15)
| | agg sum((#22 * (100dec - #23)))
| Project (#0, #3, #1, #2)
```

Many operators need to refer to columns in their input. These are displayed like
`#3` for column number 3. (Columns are numbered starting from column 0).

If you are explaining view creation for a view or one-off query that refers to unmaterialized sources, you may see `Filter` or `Project` operators attached to the unmaterialized source. This indicates that those operators will run before the upsert operator, and the upsert operator will not maintain state corresponding to data removed by the filter and project operators.

Here's an example of a materialized query pulling from unmaterialized sources and the `EXPLAIN PLAN` command:

```
CREATE MATERIALIZED VIEW mv as SELECT a, c FROM data EXCEPT ALL SELECT a, c FROM data2 where d is null;

EXPLAIN PLAN FOR VIEW mv;
```

First, Materialize reads from two unmaterialized sources, `u1` and `u3`. You can see the `Filter` and `Project` operators applied during this process.

```
Source materialize.public.data (u1):
| Project (#0, #2)

Source materialize.public.data2 (u3):
| Filter isnull(#3)
| Project (#0, #2, #3)
```

Next, Materialize runs the query on the ingested sources. Because the `FILTER` and `PROJECT` operators have already been applied during the ingestion, they don't need be run again.

```
Query:
%0 =
| Get materialize.public.data (u1)
| Project (#0, #2)

%1 =
| Get materialize.public.data2 (u3)
| Filter isnull(#3)
| Project (#0, #2)
| Negate

%2 =
| Union %0 %1
| Threshold
```

Each operator can also be annotated with additional metadata. The most common
example is the choice of implementation in the `Join` operator.

```
%3 =
| Join %0 %1 %2 (= #8 #17) (= #0 #9)
| | implementation = DeltaQuery
| |   delta %0 %1.(#1) %2.(#0)
| |   delta %1 %0.(#0) %2.(#0)
| |   delta %2 %1.(#0) %0.(#0)
| Filter (#6 = "BUILDING"), (#12 < 1995-03-15), (#27 > 1995-03-15)
| Reduce group=(#8, #12, #15) sum((#22 * (100dec - #23)))
| Project (#0, #3, #1, #2)
```

Finally, a plan can optionally have a finishing action which can sort, limit and
project the data. This is executed outside of the dataflow engine, allowing many
simple queries to just hit an existing index instead of installing a temporary
dataflow.

```
Finish order_by=(#1 desc, #2 asc) limit=none offset=0 project=(#0..=#3)
```

#### Operators in decorrelated and optimized plans

Operator | Meaning | Example
---------|---------|---------
**Constant** | Always produces the same collection of rows | `Constant (1)`
**Get** | Produces rows from either an existing source/view or from a previous operator in the same plan | `Get materialize.public.ordered (u2)`
**Project** | Produces a subset of the columns in the input rows | `Project (#2, #3)`
**Map** | Appends the results of some scalar expressions to each row in the input | `Map (((#1 * 10000000dec) / #2) * 1000dec)`
**FlatMap** | Appends the result of some table function to each row in the input | `FlatMap jsonb_foreach(#3)`
**Filter** | Remove rows of the input for which some scalar predicates return false | `Filter (#20 < #21)`
**Join** | Returns combinations of rows from each input whenever some scalar predicates are true | `Join %1 %4 (= #0 #9)`
**Reduce** | Groups the input rows by some scalar expressions, reduces each groups using some aggregate functions and produce rows containing the group key and aggregate outputs | `Reduce group=(#5) countall(null)`
**TopK** | Groups the inputs rows by some scalar expressions, sorts each group using the group key, removes the top `offset` rows in each group and returns the next `limit` rows | `TopK group=() order=(#1 asc, #0 desc) limit=5 offset=0`
**Negate** | Negates the row counts of the input. This is usually used in combination with union to remove rows from the other union input. | `Negate`
**Threshold** | Removes any rows with negative counts. | `Threshold`
**Union** | Sums the rows counts of both inputs | `Union %2 %3`
**ArrangeBy** | Indicates a point that will become an arrangement in the dataflow engine | `ArrangeBy (#0) (#3)`
**Let**  | Marks a branch of computation whose result is used later by other branches with `%0`. |  `%0 = Let 10 =`


### Reading raw plans

Raw plans are similar to decorrelated/optimized plans, but may also contain
subqueries. For example:

``` sql
EXPLAIN RAW PLAN FOR SELECT (SELECT 1)
```

```
0 =
| Constant ()
| Map select(%1)
| |
| | 1 =
| | | Constant ()
| | | Map 1
| | | Project (#0)
| | | Map
| | | Project (#0)
| |
| Project (#0)
| Map
```

The `select(%1)` indicates that the subquery block %1 below is run for each row
in the input. The outputs of each run are unioned together and then passed to
the `select` operator.

Inside a subquery, scalar expressions can refer to columns of the outside rows
by adding a `^` to the column number eg `#^1`.

#### Operators in raw plans

Operator | Meaning | Example
---------|---------|---------
**Constant** | Always produces the same collection of rows | `Constant (1)`
**Get** | Produces rows from either an existing source/view or from a previous operator in the same plan | `Get materialize.public.ordered (u2)`
**Project** | Produces a subset of the columns in the input rows | `Project (#2, #3)`
**Map** | Appends the results of some scalar expressions to each row in the input | `Map (((#1 * 10000000dec) / #2) * 1000dec)`
**CallTable** | Appends the result of some table function to each row in the input | `CallTable generate_series (#3)`
**Filter** | Remove rows of the input for which some scalar predicates return false | `Filter (#20 < #21)`
**Join** | Perform one of INNER / LEFT / RIGHT / FULL OUTER on the two inputs, using the given predicate | `InnerJoin %0 %1 on (#1 = #2)`
**Reduce** | Groups the input rows by some scalar expressions, reduces each group using some aggregate functions and produce rows containing the group key and aggregate outputs. In the case where the group key is empty and the input is empty, returns a single row with the aggregate functions applied to empty rows. | `Reduce group=(#5) agg countall(null)`
**Distinct** | Remove duplicate copies of input rows | `Distinct`
**TopK** | Groups the inputs rows by some scalar expressions, sorts each group using the group key, removes the top `offset` rows in each group and returns the next `limit` rows | `TopK group=() order=(#1 asc, #0 desc) limit=5 offset=0`
**Negate** | Negates the row counts of the input. This is usually used in combination with union to remove rows from the other union input. | `Negate`
**Threshold** | Removes any rows with negative counts. | `Threshold`
**Union** | Sums the rows counts of both inputs | `Union %2 %3`
