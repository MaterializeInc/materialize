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
