# Attribution and profiling

- Associated: https://github.com/MaterializeInc/database-issues/issues/6551

## The Problem

It is difficult to understand query performance, namely: which part of
the query is responsible for poor performance?

There are three stakeholders who want to understand query performance:

  - **customers**, who care about the queries they run;
  - **field engineering**, who support customers;
  - **the cluster team**, who support field engineering and
    (independently) want to understand how queries behave to evaluate
    their work.

Existing introspection views make it possible to understand dataflow
performance; writing WMR queries over these introspection views
suffices to get useful information about dataflow performance.

With some (??? how much/how tolerable ???) work, the cluster team and
field engineering can attribute this profile information to parts of
MIR or even SQL. Improvements to rendering have made the names of
dataflows more meaningful, but there is still a substantial gap
between dataflows and the MIR.

It is not clear to what extent customers can understand the
introspection views or the WMR queries aggregating their information.

## Success Criteria

A stakeholder can run a command like:

```
EXPLAIN PLAN PROFILE FOR INDEX my_crazy_view_idx;
```

and receive some output that explains how `my_crazy_view_idx` is
currently running, i.e., what operations does it perform and how well
are those operations performing (in terms of, e.g., memory or CPU
usage, hydration status, number of input rows for each operator,
worker skew).

In an ideal solution, all three stakeholders will receive output that
is legible at their level of sophistication.

There are three criteria that determine how legible the output is:

 - **sophistication** Optimizer engineers are prepared to deal with
   low-level details of the process, but others may not be prepared to
   deal with such details.

 - **education** Documentation and other materials can clarify
   meanings and offer examples and explanation.

 - **design** Our choices in how we represent the output and the
   quality of our implementation.

## Approaches

What structures do we attribute profiling information to? We could
annotate dataflows; we could annotate the original SQL query. Our
current architecture admits five possibilities:

  - (**dfl**) `EXPLAIN PLAN PROFILE` is a macro around a WMR analysis
    query; it describes dataflows. Attribution is direct.

  - (**lir**) `EXPLAIN PLAN PROFILE` annotates an LIR plan with
    information mapped back from the dataflows. Attribution is more or
    less direct.

  - (**mir**) `EXPLAIN PLAN PROFILE` annotates an MIR plan with
    information mapped back from the dataflows. Attribution requires
    some mapping between MIR and LIR; the proposed 'address
    predicting' forward mapping should support this.

  - (**hir**) `EXPLAIN PLAN PROFILE` annotates an HIR plan with
    information mapped back from the dataflows. Attribution requires
    the MIR->LIR mapping as well as some mapping from HIR->MIR. The
    current lowering is not careful about provenance; we would need to
    add source location information at the HIR level and track
    provenance at the MIR level, which would be a large change to the
    `transform` crate.

  - (**sql**) `EXPLAIN PLAN PROFILE` annotates the original SQL query
    information mapped back from the dataflows. Attribution requires
    HIR->MIR->LIR mapping; we would additionally need source location
    information from the SQL parser, another large change.

Each of these approaches is differently legible; as we move closer to
the SQL query itself, implementation complexity increases.

| Approach | Cluster? | FieldEng? | Customer? | Impl. Complexity |
| -------- | -------- | --------- | --------- | ---------------- |
| dfl      |        ~ |         ~ |         X |              low |
| lir      |        + |         + |         X |              low |
| mir      |        + |         + |         ~ |           medium |
| hir      |        ~ |         ~ |         + |             high |
| sql      |        ~ |         ~ |         + |           v high |

It seems unrealistic to expect customers to understand dataflows or
LIR as they exist now. MIR is a plausible thing to show customers (per
field engineering), though we lack documentation.

HIR and SQL will be more legible to customers, but the current
approach to lowering will make attribution at these levels _less_
useful for the cluster team and field engineering.

## Out of Scope

Designing a new level---DDIR, an MIR replacement---is too much for a
first cut. Refining an existing level is a good idea---but not the
first step.

## Solution Proposal

We should attribute at the MIR level: it offers the best mix of
abstraction and legibility at only a moderate level of complexity.

There is good precedent for choosing MIR, as MIR is cognate to
`EXPLAIN (FORMAT TEXT)` from Postgres, `EXPLAIN FORMAT=TREE` from
MySQL, or `EXPLAIN USING TEXT` from Snowflake.

We should identify a partner customer who can help us refine the
output of `EXPLAIN` in general and `EXPLAIN PLAN PROFILE` in
particular, rather than attempting to guess at the right information
to show.

## Implementation strategy

We will use introspection sources to do the source mapping. At present,
we have introspection sources that let us map dataflow addresses to
their statistics and operators.

Each LIR operator has a distinct (to that query) `LirId`, a `u64`
value generated when lowering MIR to LIR.

Each MIR operator has a distinct index in the post-order traversal;
we can call this index its `MirId`.

We will add introspection sources:

 - mapping `GlobalId`s and `LirId`s to dataflow addresses
 - mapping `GlobalId`s and `LirId`s to the LIR operator (think: a
   single line of an `EXPLAIN PHYSICAL PLAN`)
 - mapping `GlobalId`s and `MirId`s to an `LirId`
 - mapping `GlobalId`s and `MirId`s to the MIR operator (think: a
   single line of an `EXPLAIN PLAN`)

With these introspection sources, we can implement `EXPLAIN PLAN
PROFILE FOR (INDEX|MATERIALIZED VIEW) [name]` as a query not unlike
the existing ad-hoc WMR used to analyze dataflow. That query will:

 - resolve `name` to a `GlobalId`
 - run a query that:
   + joins (for that `GlobalId`) the `MirId`, the MIR operator, the
     `LirId`, the dataflow address, and the dataflow statistics
   + sorts appropriately (by `MirId` descending will give you
     post-order)
   + indents appropriately (cf. the existing WMR query)

We might see output like the following (totally made up numbers, query
drawn from `joins.slt`; plan seems weird to me, not relevant):

```
> CREATE MATERIALIZED VIEW v AS SELECT name, id FROM v4362 WHERE name = (SELECT name FROM v4362 WHERE id = 1);
> EXPLAIN PLAN PROFILE FOR MATERIALIZED VIEW v;
|operator                                                        |memory|
|----------------------------------------------------------------|------|
|Project (#0, #1)                                                |      |
|  Join on=(#0 = #2) type=differential                           |      |
|    ArrangeBy keys=[[#0]]                                       |  12GB|
|      ReadStorage materialize.public.t4362                      |      |
|    ArrangeBy keys=[[#0]]                                       |  10GB|
|      Union                                                     |      |
|        Project (#0)                                            |      |
|          Get l0                                                |      |
|        Map (error("more than one record produced in subquery"))|      |
|          Project ()                                            |      |
|            Filter (#0 > 1)                                     |      |
|              Reduce aggregates=[count(*)]                      |   3GB|
|                Project ()                                      |      |
|                  Get l0                                        |      |
|cte l0 =                                                        |      |
|  Filter (#1 = 1)                                               |      |
|    ReadStorage materialize.public.t4362                        |      |
```

There are pros and cons to this approach.

 + Minimal new code (just the new tables and the logic for the
   `EXPLAIN PLAN PROFILE` macro).
 + Avoids tricky reentrancy: if we implemented `EXPLAIN PLAN PROFILE`
   inside environmentd, we'd need to have a way to read from
   introspection sources while running an `EXPLAIN`.
 + Adaptable: we can run custom queries or edit the macro easily. We
   can therefore experiment a little more freely with how we render
   different MIR operators.

 - Doesn't hook in to existing `EXPLAIN` infrastructure.

If we find we miss features from the `EXPLAIN` infrastructure, we can
enrich what we put in the `MirId` operator mapping.

As an extension, we can support `EXPLAIN PLAN PROFILE ... AS SQL`,
which would generate and print the attribution query without actually
running it; this allows us to edit that query as we please.

## Open questions

We must be careful to show profiles and plans for the existing,
running dataflows. For example, suppose the user creates
`my_crazy_view` on top of `my_perfectly_sane_view` and then creates
`my_crazy_view_idx`. Later on, they create an index for
`my_perfectly_sane_view` called `my_perfectly_sane_view_idx`.  The
running dataflow for `my_crazy_view_idx` will not use
`my_perfectly_sane_view_idx`, but if we were to recreate the index it
might!

There are several possible profiling metrics to track---current memory
usage, peak memory usage, current number of rows, row throughput,
worker skew, latency (average, P50, P99, max), timestamp. Which should
we do first? (I would propose some form of memory usage and some form
of latency.)

If we commit to implementing `EXPLAIN PLAN PROFILE` at the MIR level,
how hard is it to _also_ implement it for LIR along the way?
