# Efficient Window Functions

By “window functions”, this document means the `OVER` clause, e.g.,
`SELECT row_number() OVER (PARTITION BY col1 ORDER BY col2) FROM relation;`

**(Note that [temporal windows](https://materialize.com/docs/sql/patterns/temporal-filters/) are entirely different from what we discuss in this doc. Our support for those is already quite fine. There is no need to use SQL’s `OVER` clause for those.)**

# Overview

[Many users want to use window functions](https://www.notion.so/Window-Functions-Use-Cases-6ad1846a7da942dc8fa28997d9c220dd), but our current window function support is very inefficient: We recompute results for an entire window partition for any small change in the partition. So the only situations when our current support works is if the window partitions are either very small, or they rarely change.

## Goals

We would like to have efficient window function support.

Some window functions are impossible to efficiently support in streaming, because sometimes small input changes cause big result changes. (E.g., if a new first element of a partition appears, then ROW_NUMBERs will change for the whole window partition.) So a realistic goal would be to support at least those cases where a small input change leads to a small output change.

- LAG/LEAD (i.e., previous/next element of the window partition)
    - We aim for only offset 1 in the first version, which is the default. Bigger offsets not seen in user queries yet, but shouldn’t be a problem to add support later.
    - IGNORE NULLS should be supported. (already seen in a user query) (easy)
- Window aggregations
    - Small frames: small output changes for small input changes.
    - Large frames: These are often impossible to support efficiently in a streaming setting, because small input changes might lead to big output changes. However, there are some aggregations which don’t necessarily result in big output changes even when applied with a large frame (Prefix Sum will automagically handle these cases efficiently):
        - MIN/MAX, if usually the changed input element is not the smallest/largest.
        - SUMming an expression that is often 0.
        - Window aggregations with an UNBOUNDED PRECEDING frame are fine if changes happen mostly at the end of partitions
            - e.g., OVER an `ORDER BY time` if new elements are arriving typically with fresh timestamps. Such OVER clauses are popular case in our [use case list](https://www.notion.so/Window-Functions-Use-Cases-6ad1846a7da942dc8fa28997d9c220dd).
- FIRST_VALUE / LAST_VALUE / NTH_VALUE with various frames. These are similar to window aggregations.
- ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE: These are impossible to implement efficiently in a streaming setting in many cases, because a small input change will lead to a big output change if the changed record is not near the end of the window partition. However, I can imagine some scenarios where the user knows some special property of the input data that ensures that small input changes will lead to small output changes, so she will use one of these functions and expect it to be efficient:
    - If changes mostly come near the end of the window partition. For example, if there is an ORDER BY time, and new records usually have recent timestamps. (Prefix Sum will handle this fine.)
    - If most changes are not record appearances or disappearances, but existing records changing in a way that they move only a little bit in the ordering. In this case, the output changes only for as many records, that got “jumped over” by the changing record. (Prefix Sum will handle this fine.)
    - TopK is an important special case: This is when there is a `WHERE ROW_NUMBER() < k`
        - Instead of doing Prefix Sum, we will transform this into our [efficient TopK implementation](https://www.notion.so/e62fe2b3d8354052ac7d0fe92be1e711).

## Non-goals / Limitations

- We don’t handle such OVER clauses where the ORDER BY inside the OVER is on a String or other complex type. See a discussion on supported types below in the "Types" section.
- In cases that we handle by Prefix Sum, the groups specified by the composite key of the PARTITION BY and the ORDER BY should be small, see the "Duplicate Indexes" section.

# Details

## Current state

The current way of execution is to put entire partitions into scalars, and execute the window function to all elements by a “scalar aggregation”. This happens in the HIR to MIR lowering, i.e., MIR and LIR don’t know about window functions.

## Proposal

We will use [DD’s prefix_sum](https://github.com/TimelyDataflow/differential-dataflow/blob/master/src/algorithms/prefix_sum.rs) (with some generalizations) for most of the window functions. The bulk of this work will be applied in the rendering, but we have to get the window functions from SQL to the rendering somehow.
Currently, the direct representation of window functions disappears during the HIR-to-MIR lowering, and is instead replaced by a pattern involving a `Reduce`, a `FlatMap` with an `unnest_list`, plus some `record` trickery inside `MirScalarExpr`. For example:

```c
materialize=> explain select name, pop, LAG(name) OVER (partition by state order by pop)
from cities;
                                          Optimized Plan
--------------------------------------------------------------------------------------------------
 Explained Query:                                                                                +
   Project (#3..=#5)                                                                             +
     Map (record_get[1](#1), record_get[0](#2), record_get[2](#2), record_get[0](#1))            +
       FlatMap unnest_list(#0)                                                                   +
         Project (#1)                                                                            +
           Reduce group_by=[#1] aggregates=[lag(row(row(row(#0, #1, #2), row(#0, 1, null)), #2))]+
             Get materialize.public.cities                                                       +
```

To avoid creating a new enum variant in MirRelationExpr, we will recognize the above pattern during the MIR-to-LIR lowering, and create a new LIR enum variant for window functions. I estimate this pattern recognition to need about 15-20 if/match statements. It can happen that this pattern recognition approach turns out to be too brittle: we might accidentally leave out cases when the pattern is slightly different due to unrelated MIR transforms, plus we might break it from time to time with unrelated MIR transform changes. If this happens, then we might reconsider creating a new MIR enum variant later. (Which would be easier after the optimizer refactoring/cleanup.) For an extended discussion on alternative representations in HIR/MIR/LIR, see the [Alternatives](#alternatives) section.

Also, we will want to entirely transform away certain window function patterns, most notably, the ROW_NUMBER-to-TopK transform. For this, we need to canonicalize scalar expressions, which I think we usually do in MIR. This means that this transform should happen on MIR. This will start by again recognizing the above general windowing pattern, and then performing pattern recognition of the TopK pattern.

In the rendering, we’ll use several approaches to solve the many cases mentioned in the “Goals” section:

1. We’ll use [DD’s prefix_sum](https://github.com/TimelyDataflow/differential-dataflow/blob/master/src/algorithms/prefix_sum.rs) with some tricky sum functions.
2. As an extension of 1., we’ll use a generalization of DD’s prefix sum to arbitrary intervals (i.e., not just prefixes).
3. We'll transform away window functions in some special cases (e.g., to TopK, or a simple grouped aggregation + self-join)
4. Initially, we will resort to the old window function implementation in some cases, but this should become less and less over time. I think it will be possible to eventually implement all window function usage with the above 1.-3., but it will take time to get there.

We’ll use the word **index** in the below text to mean the values of the ORDER BY column of the OVER clause, i.e., they are simply the values that determine the ordering. (Note that it’s sparse indexing, i.e., not every number occurs from 1 to n, but there are usually (big) gaps.)

----------------------

### How to handle each window function

Now I’ll list all window functions, and how we’ll support them with one of the above approaches. For a list of window functions, see https://www.postgresql.org/docs/current/functions-window.html

#### 1. Frameless window functions

These either operate on an entire partition, e.g., ROW_NUMBER, or grab a value from a specific other row, e.g., LAG.

##### 1.a. LAG/LEAD

For LAG/LEAD with an offset of 1, the sum function will just remember the previous value if it exists, and None if it does not. (And we can have a similar one for LEAD.) This has the following properties:

- It's associative.
- It's not commutative, but that isn't a problem for Prefix Sum.
- The zero element is None. Note: prefix sum sometimes sums in the zero element not just at the beginning, but randomly in the middle of an addition chain. E.g., when having *a, b, c* in the prefix then we might expect simply *a+b+c* or maybe *z+a+b+c* to be the prefix sum, but actually DD's Prefix Sum implementation might give us something like *z+a+b+z+c+z*.

I built [a small prototype outside Materialize](https://github.com/ggevay/window-funcs), where I verified that the output values are correct, and that output is quickly updated for small input changes.

For LAG/LEAD with *k > 1* (which computes the given expression not for the previous record, but for the record that was *k* records ago), the sum function could simply remember the last *k* values, acting on a `Vec<Val>` of length at most *k*, which would generalize `Option<Val>`. This works kind of ok for small *k*. A more complicated but probably better solution is to find the index for that element in the window partition that is *k* elements behind by using the same method as we use for calculating the intervals of the framed window functions (see below). Then with the index in hand, we can just do a self-join.

##### 1.b. ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE

There is the **TopK** special case, i.e., where the user specifies `ROW_NUMBER() <= k` (or similar). We want to transform this pattern to our efficient TopK implementation, rather than using prefix sum. This should probably be an MIR transform. This way we can rely on MirScalarExpr canonicalization when detecting different variations of `rownum <= k`, e.g., `k >= rownum`, `rownum < k+1`, `rownum - 1 < k`.

In most situations other than TopK, these functions cannot be implemented efficiently in a streaming setting, because small input changes often lead to big output changes. However, as noted in the [Goals](#Goals) section, there are some special cases where small input changes will lead to small output changes. These will be possible to support efficiently by performing a Prefix Sum with an appropriate sum function.

#### 2. Window aggregations

These operate on so-called **frames**, i.e., a certain subset of a window partition. Frames are specified in relation to the current row. For example, "sum up column `x` for the preceding 5 rows from the current row". For all the frame options, see https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-WINDOW-FUNCTIONS

There is a special case where the frame includes the entire window partition: An aggregation where the frame is both UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING at the same time (or there is no ORDER BY, which has a similar effect) should be transformed to a grouped aggregation + self join.

In all other cases, we’ll use prefix sum, for which we need to solve two tasks:

*I.* We have to find the end(s) of the interval that is the frame. I.e., we need to tell **indexes** to Prefix Sum (where the index is a value of the ORDER BY column(s), as mentioned above).

*II.* We’ll need to generalize Prefix Sum to not just prefixes, but arbitrary intervals. (A prefix interval is identified by one index, a general interval is identified by two indexes.)

*Solving I. for each framing mode (RANGE | GROUPS | ROWS):*
- RANGE: this is the obvious one (but probably not the most often used): The offset is just a difference in the (sparse) “index” of the prefix sum (i.e., the ORDER BY column).
    - Btw. we might translate some inequality self-joins to this one!
- GROUPS: (One could say that this is probably not so often used, so no need to initially support it. However, the problem is that the solution for ROWS will probably build on this, and that is the default, so that one is often used.) We have to somehow translate the offset to a difference in the “index” of the prefix sum:
    - Naive solution: Since this is a difference in DENSE_RANK between the current row and the ends of the interval, we could compute a DENSE_RANK, and then join with that to find the “index” (the value of the ORDER BY column).
        - However, this would be slow, because DENSE_RANK is inherently not well incrementalizable: Small input changes can lead to lots of changes in the ranks.
    - **(Tricky part)** A better solution is to calculate a count aggregation on all ranges (with prefix sum’s `aggregate`) (let's call this `counts`), and then do a logarithmic search for the index with a nested `iterate`:
        - We start with `(index, offset)` pairs for all the possible current elements in parallel, where the pair means that we need to move `index` down by `offset` (down when looking for the lower end of the interval, or up when looking for the upper end), i.e., we need to lower `index` while lowering `offset` to 0.
        - Then, at every step of an `iterate`, we can use a range from `counts` on each  `(index, offset)` pair: `index` is lowered by the size of the range, and `offset` is lowered by the aggregated count of the range.
            - We want to use the biggest such range in `counts` that doesn't make `offset` negative. We can do this by an inner `iterate`.
- ROWS: similar to GROUPS, but use indexes that include the deduplication component. (see below at “Duplicate indexes”)

There is also `frame_exclusion`, which sometimes necessitates special handling for the group that contains the current row. In such cases, we will put together the result value of the window function from 3 parts: 1. prefix sum (generalized to arbitrary intervals) for groups that are earlier than the current row’s group, 2. prefix sum for groups that are later than the current row’s group, 3. current row’s group (without prefix sum).

*Solving II.:*

For invertible aggregation functions (e.g., sum, but not min/max) we can use the existing prefix sum with a minor trick: agg(a,b) = agg(0,b) - agg(0,a).
    - However, the performance of this might not be good, because even if (a,b) is a small interval, the (0,a) and the (0,b) intervals will be big, so there will be many changes of the aggregates of these even for small input changes.

To have better performance (and to support non-invertible aggregations, e.g., min/max), we need to extend what the `broadcast` part of prefix sum is doing (`aggregate` can stay the same):
    - `queries` will contain intervals specified by two indexes.
    - `requests`: We can similarly compute a set of requests from `queries`. The change will only be inside the `flat_map`.
    - `full_ranges`, `zero_ranges`, `used_ranges` stay the same.
    - `init_states` won’t start at position 0, but at the lower end of the intervals in `queries`
    - The iteration at the end will be mostly the same.

#### 3. FIRST_VALUE / LAST_VALUE / NTH_VALUE

These also operate based on a **frame**, similarly to window aggregations (see above). They can be similarly implemented to window aggregations, i.e., we could “sum” up the relevant interval (that is not necessarily a prefix) with an appropriate sum function.

Alternatively, we could make these a bit faster (except for NTH_VALUE) if we just find the index of the relevant end of the interval (i.e., left end for FIRST_VALUE), and then self-join.

(And there are some special cases when we can transform away the window function usage: FIRST_VALUE with UNBOUNDED PRECEDING and LAST_VALUE with UNBOUNDED FOLLOWING should be transformed to just a (non-windowed) grouped aggregation + self-join instead of prefix sum trickery. Also, similarly for the case when there is no ORDER BY.)

----------------------

### Duplicate indexes

There might be rows which agree on both the PARTITION BY key and the ORDER BY key (the index). Such duplicate indexes are not handled by Prefix Sum. To eliminate these duplicates, we will number the elements inside each group with 0, 1, 2, …, and this will be an additional component of the prefix sum indexes.

For this to perform well, we are assuming that groups are small.
This is not an unreasonable assumption, because a group is identified here by a value of the PARTITION BY expression + a value of the ORDER BY expression.
Also note that if there is no ORDER BY, then groups might be large, but in this case we don’t employ Prefix Sum, but we transform away the window functions to grouped aggregation + self-join (as noted above).

Alternatively, we could change Prefix Sum so that it correctly handles duplicate indexes.

### Parallelism

DD's Prefix Sum should be data-parallel even inside a window partition. (It’s similar to [a Fenwick tree](https://en.wikipedia.org/wiki/Fenwick_tree), with sums maintained over power-of-2 sized intervals, from which you can compute a prefix sum by putting together LogN intervals.) TODO: But I wasn't able to actually observe a speedup in a simple test when adding cores, so we should investigate what’s going on with parallelization. There was probably just some technical issue, because all operations in the Prefix Sum implementation look parallelizable.

### Types

We'll have to generalize DD's Prefix Sum to orderings over types other than a single unsigned integer, which is currently hardcoded in the code that forms the intervals. We’ll map other types to a single unsigned integer. Importantly, this mapping should *preserve the ordering* of the type:

- Signed integer types are fine, we just need to fiddle with the sign to map them to an unsigned int in a way that preserves the ordering.
- Date/Time types are just a few integers. We’ll concatenate their bits.
- I don’t know how to handle strings, so these are out of scope for now. (Not seen in user queries yet.)

## Alternatives

### Where to put the idiom recognition?

We could do the idiom recognition in the rendering instead of the MIR-to-LIR lowering. However, the lowering seems to be a more natural place for it:
- We shouldn't have conditional code in the rendering, and this idiom recognition will be a giant piece of conditional code,
- We want EXPLAIN PHYSICAL PLAN to show how we'll execute a window function.
- We need to know the type of the ORDER BY columns, which we don't know in LIR. (Although we could add extra type info to window function calls.)

### Representing window functions in each of the IRs

There are several options for how to represent window functions in HIR, MIR, and LIR. For each of the IRs, I can see 3 options:

1. Create a new relation expression enum variant. This could be a dedicated variant just for window functions, or it could be a many-to-many Reduce, which would initially only handle window functions, but later we could also merge `TopK` into it. (Standard Reduce is N-to-1, TopK is N-to-K, a window function is N-to-N. There are differences also in output columns.)
2. Hide away window functions in scalar expressions. (the current way in HIR)
3. Reuse an existing relation expression enum variant, e.g., `Reduce`.

#### HIR

In HIR, the window functions are currently in the scalar expressions (option 2. from above), but it’s possible to change this.

1. *Dedicated `HirRelationExpr` variant:*
    - There is a precedent for a similar situation: HIR has aggregation expressions, which (similarly to window expressions) have the property that they are in a scalar expression position, but their value is actually calculated by a dedicated `HirRelationExpr` variant (`Reduce`), and then there is just a column reference in `HirScalarExpr`.
2. *Hiding in `HirScalarExpr`:*
    - HIR wants to be close to the SQL syntax, and window functions appear in scalar position in SQL.
    - It’s already implemented this way, so if there is no strong argument for 1. or 3., then I’d like to just leave it as it is.
3. *Reusing `Reduce`*

#### MIR

We need an MIR representation for two things:

- To get the window function expressions to rendering, where we’ll apply the prefix sum. (Alternatively, we could recover window functions from the patterns that get created when the current HIR lowering compiles away window functions.)
- To have optimizer transforms for some important special cases of window functions, e.g., for TopK patterns. (Alternatively, we could apply these transforms in the HIR-to-MIR lowering.)
1. *Create a dedicated enum variant in `MirRelationExpr`:*
    - I think this is better than 2., because Map (and MirScalarExprs in general) should have the semantics that they can be evaluated by just looking at one input element, while a window function needs to look at large parts of a window partition. If we were to put window functions into scalar expressions, then we would need to check lots of existing code that is processing MirScalarExprs that they are not getting unpleasantly surprised by window functions.
    - Compared to 3., it might be easier to skip window functions in many transforms. This is both good and bad:
        - We can get a first version done more quickly. (And then potentially add optimizations later.)
        - But we might leave some easy optimization opportunities on the table, which would come from already-existing transform code for `Reduce`.
   - A new `MirRelationExpr` variant would mean we have to modify about 12-14 transforms `LetRec` is pattern-matched in 12 files in the `transform` crate, `TopK` 14 times. See also [the recent LetRec addition](https://github.com/MaterializeInc/materialize/commit/9ac8e060d82487752ba28c42f7b146ff9f730ca3) for an example of how it looks when we add a new `MirRelationExpr` variant. (But note that, currently, LetRec is disabled in all but a few transforms)
   - When considering sharing a new many-to-many Reduce variant between window functions and TopK, we should keep in mind that the output columns are different: TopK keeps exactly the existing columns, but a window function adds an extra column.
2. An argument can also be made for hiding window functions in `MirScalarExpr`:
    - This seems scary to me, because scalar expressions should generally produce exactly one value by looking at exactly one record, which is not true for window functions. It's hard to tell that none of the code that is dealing with scalar expressions would suddenly break.
    - `MirScalarExpr` can occur in several places (JoinClosure, etc.), so we would have to attend to window functions in the lowerings of each of these.
    - However, there is a precedent for scalar expressions that don't exactly fit the "1 value from 1 record" paradigm: the temporal stuff.
3. We could consider putting window functions in `MirRelationExpr::Reduce`. This was suggested by Frank: [https://materializeinc.slack.com/archives/C02PPB50ZHS/p1672685549326199?thread_ts=1671723908.657539&cid=C02PPB50ZHS](https://materializeinc.slack.com/archives/C02PPB50ZHS/p1672685549326199?thread_ts=1671723908.657539&cid=C02PPB50ZHS)
    - `Reduce` is pattern-matched in 20 files in the `transform` crate. All of these will have to be modified.
        - This is a bit more than the ~12-14 pattern matches of adding a new enum variant, because there are some transforms specialized to `Reduce`, which we wouldn't need to touch if it were a new enum variant instead.
    - We could maybe reuse some of the code that is handling `Reduce`? But we have to keep in mind two big differences between grouped aggregation and window functions:
        - Grouped aggregation produces exactly one row per group.
            - But Frank is saying that we could generalize `Reduce` to make it many-to-many, [as in DD’s `reduce`](https://github.com/TimelyDataflow/differential-dataflow/blob/master/src/operators/reduce.rs#L71).
                - Btw. matching up MIR Reduce’s behavior with DD’s Reduce would be important if the translation of MIR’s Reduce would be to just call DD’s Reduce, but this is not the case at all for window functions.
        - The output columns are different: A grouped aggregation’s output is the grouping key columns and then one column for each aggregate, but a window function retains all columns, and then just appends one column at the end (regardless of the grouping key).
    - It seems to me that the overlap between current `Reduce` handling and how to handle window functions is not big enough to justify putting the window functions into `Reduce`. There would be ifs every time we handle `Reduce`, and different things would be happening for traditional `Reduce` and window function `Reduce`.
      - We could later have a separate EPIC to consider unifying window function and Reduce (and potentially TopK) into a many-to-many “super-reduce”.
    - Example transformations:
      - `ColumnKnowledge`
          - The `optimize` call for the `group_key` could be reused (for the key of the PARTITION BY), but this is just a few lines.
              - But they cannot be pushed to the `output` `Vec<DatumKnowledge>`, because the grouping key columns are not part of the output. Instead, the knowledge from the original columns should be pushed.
          - The rest of the code is also similar to what needs to happen with window functions, but not exactly the same, due to the more complicated representation of window expressions (`WindowExprType`) vs. aggregate expressions. (`AggregateExpr`). So, it seems to me that code sharing wouldn't really help here.
      - `FoldConstants`: The heavy lifting here is in `fold_reduce_constant`, which is completely different from what is needed for window functions. The rest of the code is similar, but not identical.
      - `JoinImplementation`: This tries to reuse arrangements after a Reduce, which we cannot do for window functions. So we would have to special-case those Reduces that are actually window functions.
      - `MonotonicFlag` is easy either way.
      - `ReduceElision` could be applied (by adding some ifs due to the different output columns). We would have to implement an `on_unique` for window functions as well. (Although, this one doesn't sound like a terribly useful optimization for window functions, because it’s hard to see how a window function call could end up on a unique column…)
      - `LiteralLifting`: The two inlinings at the beginning could be reused. (But those are already copy-pasted ~5 times, so they should rather be factored out into a function, and then they could be called when processing a new enum variant for window functions.)
      - …

#### LIR

1. We could add a dedicated LIR `Plan` enum variant. This sounds like the right approach to me, because window functions will have a pretty specific rendering (Prefix Sum) that’s distinct from all other operators, and LIR is supposed to be a close representation of what we are about to render.
2. (We can pretty much rule out hiding them in scalar expressions at this point, because scalar expressions get absorbed into operators in all kinds of ways, and we don't want to separately handle window functions all over the place.)
3. Another option is to model it as a variant of `Reduce`.
    - But I don’t see any advantage of this over an own enum variant. I don’t see any code reuse possibility between the existing `Reduce` rendering and the rendering of window functions by Prefix Sum.

# Rollout

## Testing

- Correctness:
    - Since there is already some window functions support (it’s just inefficient), there is already `window_funcs.slt` (4845 lines). However, some window functions and window aggregations (and some options, e.g., IGNORE NULLS, some tricky frames) are not supported at all currently, so those are not covered. I’ll add tests to this file for these as well.
    - Additionally, there is `cockroach/window.slt` (3140 lines), which is currently disabled (with a `halt` at the top of the file). We’ll re-enable this, when our window function support will be (nearly) complete.
    - (If we were to reuse `MirRelationExpr::Reduce` to represent window functions, then we’ll have to pay extra attention that existing transforms dealing with `Reduce` are not messing up window functions.)
    - Philip’s random-generated queries testing would be great, because there is a large number of options and window functions, so it’s hard to cover all combinations with manually written queries.
        - Also, it would be good to have randomly generated other stuff around the window functions, to test that other transforms are not breaking.
    - We'll need to extensively test the idiom recognition. Fortunately, the first step of the idiom recognition is quite easy: we just need to look at the aggregate function of the `Reduce`, and decide if it is a window function. If this first step finds a window function, then we could add a soft assert that we manage to recognize all the other parts of the pattern. This way, all the above tests would be testing also the idiom recognition (even when a test doesn't involve EXPLAIN).
      - Also, there could be a sentry warning with the above soft assert, so that we know if a customer is probably running into a performance problem due to falling back to the old window function implementation.
- Performance testing: Importantly, we need to test that we efficiently support situations when small input changes lead to small output changes.
    - Writing automated performance tests is tricky though. Currently, we don’t have any automated performance tests.
    - At least manual testing should definitely be performed before merging the PRs, since the whole point of this work is performance.
    - We could do it roughly as follows:
        - We put in lots of input data with one timestamp, as an “initial snapshot”. Processing this should be at least several seconds.
        - We change a small portion of the input data.
            - But the total size of the affected partitions should cover most of the input data. This is important, since if window partitions are very small, then the current window function support works fine.
            - This should complete orders of magnitude faster than the initial snapshot.
    - More specifically, maybe we could have a Testdrive test that performs the following steps:
        - Copy some TPC-H data from our TPC-H source into tables.
        - Create a materialized view with some window functions on the tables.
        - Do some inserts/updates/deletes on the tables.
        - Check that updating of the materialized view happens quickly.
            - Should be possible to set up the input data and the queries in such a way that
                - updating takes orders of magnitude faster than the initial snapshot.
                - But not with the current window function support.
                - The difference from the initial snapshot should be big enough so that the test won’t be flaky.
    - (I already tested a [simple prototype for LAG outside Materialize](https://github.com/ggevay/window-funcs).)
- We should measure the memory needs of Prefix Sum, so that we can advise users when sizing replicas.

## Lifecycle

There are many window functions, and many frame options. We will gradually add the new, efficient implementations for an increasing set of window function + frame setting combinations across several PRs. We will make sure the new implementation is correct and performs well before merging each of the PRs.

# Open questions

- MIR/LIR representation.

- How to have automated performance tests? Can we check in Testdrive that some materialized view (that has window functions) is being updated fast enough?
- We should check that there is correct parallelization inside window partitions, see above.
