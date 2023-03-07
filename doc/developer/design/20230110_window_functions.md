# Efficient Window Functions

By “window functions”, this document means the `OVER` clause, e.g.,

`SELECT row_number() OVER (PARTITION BY col1 ORDER BY col2) FROM relation;`

**(Note that [temporal windows](https://materialize.com/docs/sql/patterns/temporal-filters/) are entirely different from what we discuss in this doc. Our support for those is already quite fine. There is no need to use SQL’s `OVER` clause for those.)**

# Overview

[Many users want to use window functions](https://www.notion.so/Window-Functions-Use-Cases-6ad1846a7da942dc8fa28997d9c220dd), but our current window function support is very inefficient: We recompute results for an entire window partition for any small change in the partition. This means the only situations when our current support works is if the window partitions are either very small, or they rarely change.

## Window Functions

Most of SQL is based on an unordered data model, but window functions provide access to an ordered view of data. Thereby, we can access concepts such as previous or next row, nearby rows, consecutive rows, etc. This enables certain computations that are otherwise hard to express with the rest of SQL, e.g., computing differences between consecutive rows.

Window functions were introduced in the SQL:2003 standard. They compute a scalar value for each row, by using information from other, nearby rows. Exactly which other rows are involved is determined by the function and an `OVER` clause that accompanies the window function call itself. For example, the following query prints each measurement and its difference from the previous measurement, where "previous" is to be understood according to the ordering on the `time` field.
```SQL
SELECT time, measurement_value, measurement_value - LAG(measurement_value) OVER (ORDER BY time)
FROM measurements;
```
The `LAG` window function computes the value of a scalar expression (here simply a reference to `measurement_value`) on data from the previous row (instead of the current row, which is normally what a scalar expression does).
The `OVER` clause has to directly follow the window function call (`LAG(...)` here). Note that this `ORDER BY` has no influence on the ordering of the result set of the query, it merely influences the operation of `LAG`.

Note that if the measurements follow each other at regular time intervals, then the same query [can be written without a window function](https://materialize.com/docs/sql/patterns/window-functions/#laglead-for-time-series), with just a self join. However, for arbitrary measurement times there is no good workaround without a window function.

We can also add a `PARTITION BY` clause inside the `OVER` clause. In this case, the window function will gather information only from those other rows that are in the same partition. For example, we can modify the above query for the situation when measurements are from multiple sensors, and we want to compute the differences only between measurements of the same sensor:

```SQL
SELECT sensor_id, time, measurement_value, measurement_value - LAG(measurement_value)
    OVER (ORDER BY time PARTITION BY sensor_id)
FROM measurements;
```

Certain window functions operate on a _window frame_, which is a subset of a partition. The default frame includes the rows from the first row of the partition up to the current row (or more accurately, to the last row of the peer group of the current row, where a peer group is a set of rows that are equal on both the `PARTITION BY` and the `ORDER BY`). For example, all aggregation functions can be used also as window functions (we will refer to this as _window aggregations_), where they aggregate values from inside the current window frame. The following query calculates a running total (prefix sum) of measurement values for each sensor (which wouldn't make sense for a temperature sensor, but makes sense for, e.g., a water flow sensor):

```SQL
SELECT sensor_id, time, SUM(measurement_value)
    OVER (ORDER BY time PARTITION BY sensor_id)
FROM measurements;
```

Note that this query doesn't compute just one value for each partition. Instead, it calculates a value for each input row: the sum of the same sensor's measurements that happened no later than the current input row.

We can also explicitly specify a frame, i.e., how far it extends from the current row, both backwards and forwards. One option is to say `UNBOUNDED PRECEDING` or `UNBOUNDED FOLLOWING`, meaning that the frame extends to the beginning or end of the current partition. Another option is to specify an offset. For example, the following query computes a moving average (e.g., to have a smoother curve when we want to plot it or when we want less noise for an alerting use case):

```SQL
SELECT sensor_id, time, AVG(measurement_value)
    OVER (ORDER BY time PARTITION BY sensor_id
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
FROM measurements;
```

In this query, the frame extends 4 rows backwards, and ends at the current row (thus containing at most 5 rows).

The exact meaning of the offset depends on the _frame mode_:
- In `ROWS` mode (such as above), the frame extends for the specified number of rows (or less, for rows near the beginning or end of the partition).
- In `GROUPS` mode, the frame extends for the specified number of peer groups, where a peer group is a set of rows that are equal on both the `PARTITION BY` and the `ORDER BY`.
- In `RANGE` mode, the frame extends to those rows whose difference from the current row on the `ORDER BY` column is not greater than the offset (only one ORDER BY column is allowed for this frame mode). For example, the following query computes a moving average with a frame size of 5 minutes (which might be more useful than a `ROWS` offset when the measurement values are at irregular times):
```SQL
SELECT sensor_id, time, AVG(measurement_value)
    OVER (ORDER BY time PARTITION BY sensor_id
        RANGE BETWEEN '5 minutes' PRECEDING AND CURRENT ROW)
FROM measurements;
```

There is also the _frame exclusion_ option, which excludes certain rows near the current row from the frame. `EXCLUDE CURRENT ROW` excludes the current row. `EXCLUDE GROUP` excludes the current row's peer group (also excluding the current row). `EXCLUDE TIES` excludes the current row's peer group, except for the current row itself. `EXCLUDE NO OTHERS` specifies the default behavior, i.e., no exclusions.

For more details, see Postgres' documentation on window functions:
- Syntax: https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-WINDOW-FUNCTIONS
- List of window functions: https://www.postgresql.org/docs/current/functions-window.html

## Goals

We would like to have efficient window function support.

Some window functions are impossible to efficiently support in streaming, because sometimes small input changes cause big result changes. (E.g., if a new first element of a partition appears, then ROW_NUMBERs will change for the whole window partition.) So a realistic goal would be to support at least those cases where a small input change leads to a small output change.

- LAG/LEAD (i.e., previous/next element of the window partition) (these don't have any frames)
    - We aim for only offset 1 in the first version (i.e., the previous or the next element), which is the default. Bigger offsets have not been seen in user queries yet (i.e., when requesting to go back or forward by several rows).
    - IGNORE NULLS should be supported. (already seen in a user query)
- Window aggregations
    - Small frames (e.g., summing the previous 5 elements): We should support these efficiently, because a small frame means that small input changes lead to small output changes.
    - Large frames: These are often impossible to support efficiently in a streaming setting, because small input changes can lead to big output changes. However, there are some aggregations which don't necessarily result in big output changes even when applied with a large frame (Prefix Sum will automagically handle the following cases efficiently):
        - MIN/MAX, if usually the changed input element is not the smallest/largest.
        - SUMming an expression that is 0 for many rows.
        - Window aggregations with an UNBOUNDED PRECEDING frame are fine if changes happen mostly at the end of partitions
            - e.g., OVER an `ORDER BY time` if new elements are arriving typically with fresh timestamps. Such OVER clauses are popular in our [use case list](https://www.notion.so/Window-Functions-Use-Cases-6ad1846a7da942dc8fa28997d9c220dd).
    - For frames encompassing the entire window partition (i.e., an UNBOUNDED frame and/or no ORDER BY), window aggregations can be simply translated to a standard grouped aggregation + a self-join. In case of these frames, small input changes often lead to big output changes, but similar exceptions exist as listed for "Large frames" above.
- FIRST_VALUE / LAST_VALUE / (NTH_VALUE) with various frames.
  - For the case of general frames, these are similar to window aggregations.
  - For frames encompassing the entire window partition (an UNBOUNDED frame and/or no ORDER BY), FIRST_VALUE / LAST_VALUE are actually requesting the top or the bottom row of the partition. We should compile this to TopK, with k=1.
- ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE: These functions are often impossible to support efficiently in a streaming setting, because a small input change will lead to a big output change if the changed record is not near the end of the window partition. However, I can imagine some scenarios where the user knows some special property of the input data that ensures that small input changes will lead to small output changes, so she will use one of these functions and expect it to be efficient:
    - If changes mostly come near the end of the window partition. For example, if there is an ORDER BY time, and new records usually have recent timestamps. (Prefix Sum will handle this fine.)
    - If most changes are not record appearances or disappearances, but existing records changing in a way that they move only a little in the ordering. In this case, the output changes only for as many records, that got “jumped over” by the changing record. (Prefix Sum will handle this fine.)
    - TopK is an important special case (popular in our [use case list](https://www.notion.so/Window-Functions-Use-Cases-6ad1846a7da942dc8fa28997d9c220dd)): This is when there is a `WHERE ROW_NUMBER() <= k`. Instead of relying on Prefix Sum, we should transform this into our [efficient TopK implementation](https://github.com/MaterializeInc/materialize/blob/2f56c8b2ff1cc604e5bff9fb1c75a81a9dbe05a6/src/compute-client/src/plan/top_k.rs#L30).

## Non-goals

As noted above, some window function queries on some input data are impossible to efficiently support in a steaming setting: When small input changes lead to big output changes, then no matter how efficient is the implementation, even just emitting the output will take a long time. We are not aiming to efficiently support such use cases. Our docs should be clear about this, mentioning this fundamental fact about window functions near the top of the page.

## Limitations

We don't handle such OVER clauses where the ORDER BY inside the OVER is on a String or other such type that can't be mapped to a fixed-length integer. See a discussion on supported types below in the "Types" section.

# Details

## Current state

The current way of executing window functions is to put entire window partitions into scalars, and execute the window function on all elements of a partition by a "scalar aggregation". This translation happens in the HIR to MIR lowering, i.e., MIR and LIR don't know about window functions (except for a special "scalar aggregation" function for each window function). This is very inefficient for large partition sizes (e.g., anything above 100 elements), because any change in a partition means that the entire "scalar" that is representing the partition is changed.

## Proposal

We'll use several approaches to solve the many cases mentioned in the “Goals” section:

1. We'll use [DD's prefix_sum](https://github.com/TimelyDataflow/differential-dataflow/blob/master/src/algorithms/prefix_sum.rs) with some tricky sum functions and some generalizations.
2. We'll use a special-purpose rendering for LAG/LEAD of offset 1 with no IGNORE NULLS, which will be simpler and more efficient than Prefix Sum.
3. As an extension of 1., we'll use a generalization of DD's prefix sum to arbitrary intervals (i.e., not just prefixes).
4. We'll transform away window functions in some special cases (e.g., to TopK, or a simple grouped aggregation + self-join)
5. Initially, we will resort to the old window function implementation in some cases, but this should become less and less over time. I think it will be possible to eventually implement all window function usage with the above 1.-4. approaches, but it will take time to get there.

### Getting window functions from SQL to the rendering

The bulk of this work will be applied in the rendering, but we have to get the window functions from SQL to the rendering somehow. Currently, the explicit representation of window functions disappears during the HIR-to-MIR lowering, and is instead replaced by a pattern involving a `Reduce`, a `FlatMap` with an `unnest_list`, plus some `record` trickery inside `MirScalarExpr`. For example:

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

To avoid creating a new enum variant in MirRelationExpr, we will recognize the above pattern during the MIR-to-LIR lowering, and create a new LIR enum variant for window functions. I estimate this pattern recognition to need about 15-20 if/match statements. It can happen that this pattern recognition approach turns out to be too brittle: we might accidentally leave out cases when the pattern is slightly different due to unrelated MIR transforms, plus we might break it from time to time with unrelated MIR transform changes. If this happens, then we might reconsider creating a new MIR enum variant later. (Which would be easier after the optimizer refactoring/cleanup.) For an extended discussion on alternative representations in HIR/MIR/LIR, see the [Representing window functions in each of the IRs](#Representing-window-functions-in-each-of-the-IRs) section.

Also, we will want to entirely transform away certain window function patterns; most notable is the ROW_NUMBER-to-TopK transform. For this, we need to canonicalize scalar expressions, which I think we usually do in MIR. This means that transforming away these window function patterns should happen on MIR. This will start by again recognizing the above general windowing pattern, and then performing pattern recognition of the TopK pattern.

### Prefix Sum

This section defines prefix sum, then discusses various properties/caveats/limitations of DD's prefix sum implementation from the caller's point of view, and then discusses the implementation itself.

#### Definition

Prefix sum is an operation on an ordered list of input elements, computing the sum of every prefix of the input list. Formally, if the input list is

`[x1, x2, x3, ..., xn]`,

then a straightforward definition of prefix sum is

`[x1, x1 + x2, x1 + x2 + x3, ..., x1 + x2 + x3 + ... + xn]`.

However, it will be more convenient for us to use a slightly different definition, where
- the result for the ith element doesn't include the ith element, only the earlier elements, and
- there is a zero element (`z`) at the beginning of each sum:

`[z, z + x1, z + x1 + x2, ..., z + x1 + x2 + x3 + ... + x_n-1]`.

The input elements can be of an arbitrary data type, and `+` can be an arbitrary operation that is associative and has a zero element.

Note that commutativity of `+` is not required. Importantly, the result sums include the input elements in their original order, e.g., we cannot get the result `z + x2 + x1` for the 3rd input element, but `x1` and `x2` should be summed in their original order.

#### Properties of DD's prefix sum implementation

[DD's prefix sum implementation](https://github.com/TimelyDataflow/differential-dataflow/blob/master/src/algorithms/prefix_sum.rs) computes the above sum for collections of `((usize, K), D)`, where `D` is the actual data type, the usizes determine the ordering (we will need to generalize this, see the "ORDER BY types" section), and `K` is a key type. For each key, a separate prefix sum is computed. The key will be the expression of the PARTITION BY clause.

A caveat of the implementation is that extra instances of the zero element might be added anywhere in the sum. E.g., instead of `z + x1 + x2 + x3`, we might get `z + x1 + x2 + z + z + x3 + z`. Therefore, the zero element should be both a left zero and a right zero, i.e., `x + z = z + x = x` has to hold for the sum function. This is not a problematic limitation in practice, because we can add a suitable zero to any type by wrapping it in `Option`, and making `None` the zero.

As is common in distributed systems, the sum function has to be associative, because there is no guarantee that the implementation will compute a left-deep sum (e.g., `((z + x1) + x2) + x3`), but might put parenthesis anywhere in the sum, e.g., `(z + (x1 + x2)) + x3`. (But commutativity is not required, as mentioned above.)

The implementation is data-parallel not just across keys, but inside each key as well. TODO: But I wasn't able to actually observe a speedup when adding cores in a simple test, so we should investigate what’s going on with parallelization. There was probably just some technical issue in my test, because all operations in the Prefix Sum implementation look parallelizable, so it should be fine. I'll try to properly test this in the next days.

We’ll use the word **index** in this document to mean the values of the ORDER BY column of the OVER clause, i.e., they are simply the values that determine the ordering. (Note that it’s sparse indexing, i.e., not every number occurs from 1 to n, but there are usually (big) gaps.)

As mentioned above, DD's prefix sum needs the index type to be `usize`. It is actually a fundamental limitation of the algorithm that it only works with integer indexes, and therefore we will have to map other types to integers. We discuss this in the "ORDER BY types" section.

#### Implementation details of DD's prefix sum

(The reader might skip this section on a first read, and refer back to it later when delving into performance considerations, or extensions/generalizations of prefix sum needed for framed window functions and LAG/LEAD with an offset >1.)

[DD's prefix sum implementation](https://github.com/TimelyDataflow/differential-dataflow/blob/master/src/algorithms/prefix_sum.rs) is somewhat similar to a [Fenwick tree](https://en.wikipedia.org/wiki/Fenwick_tree), but relies on DD's unordered collection operations rather than a flat array, and has sparse indexing. It precomputes the sums for certain power-of-2 sized intervals (`fn aggregate`), and then computes the sum for any (not precomputed) prefix interval by dividing the interval into a logarithmic number of such intervals for which there is a precomputed sum, and adding these precomputed sums together (`fn broadcast`).

##### The `aggregate` function

The intervals included in the sum precomputations will be specified in the index domain, e.g., a certain precomputed interval might cover those input elements whose index is larger or equal than 8 and smaller than 12. The size of this interval is 4, but this size is to be understood in the index domain; there might be less than 4 actual input elements whose index falls into this interval.

We now discuss the set of intervals for which `aggregate` precomputes a sum (we'll denote this set by _A_). Importantly, we precompute the sum for only such intervals that cover at least one input element. We will define a set _A'_, of which _A_ will be the subset of such intervals that cover at least one input element. We give 3 alternative, but equivalent definitions (let's denote the bit length of indexes by _b_):

*1. (Direct definition.)*
For each _i ∈ 0 ..= b-1_, for each _j ∈ 0 .. 2^(b-i)_, let _A'_ include the interval _j * 2^i .. j*2^i + 2^i_.

*2. (A recursive definition, useful for understanding the actual implementation of `aggregate`.)*
We start the recursion by adding to _A'_ all intervals of length 1, i.e., one "interval" at each of 0 .. 2^b. Then we take _b-1_ steps, and in each of these steps we add those intervals that are twice as large as the intervals added in the previous step. Specifically, we take the set of intervals added in the previous step, and merge pairs of neighboring intervals (the pairs don't overlap). E.g., we merge the first two intervals, the 3. and 4., the 5. and 6., etc. To also compute the actual sums, when merging two intervals, we can sum their precomputed sums to compute the sum for the merged interval.

The actual implementation of `aggregate` proceeds in similar steps, but it builds _A_ directly, rather than _A'_: rather than starting from all possible 1-length intervals, it starts from only those indexes that actually occur in the input data. This means that each merge step will find that it has to merge either 1 or 2 intervals, because one of the intervals that would have participated in the merge if we were building _A'_ rather than _A_ might actually be absent, due to not containing any such index that occurs in the input data. When "merging" only 1 interval rather than 2, the sum of the "merged" interval will be the same value as that 1 constituent interval.

*3. (Index bit vector prefixes. This one is useful for certain performance considerations.)*
This definition directly defines _A_ rather than _A'_. Let's consider the indexes that occur in the input data as bit vectors. For each of _len ∈ 0 ..= b-1_, let's define the set *D_len* to be the set of distinct prefixes of length _len_ of all the index bit vectors. In other words, for each _len_, we take the first _len_ bits of each of the indexes, and form *D_len* by deduplicating all these index prefixes. Let *D* be the union of all *D_len* sets.
_A_ will have exactly one interval for each element of *D*. The actual mapping (i.e., what element of _A_ belongs to each element of *D_len*) is not so important, since we will rely on just the sizes of the *D_len* sets for certain performance considerations. (The actual mapping is as follows: For each *len*, for each *d ∈ D_len*, *A* includes the interval whose size is *b-len* and starts at _d * 2^(b-len)_.) See a performance discussion relying on this definition [here](#Performance-and-optimizations).

##### The `broadcast` function

TODO

----------------------

### How to handle each window function

Now I’ll list all window functions, and how we’ll support them with one of the above approaches. For a list of window functions, see https://www.postgresql.org/docs/current/functions-window.html

#### 1. Frameless window functions

These either operate on an entire partition, e.g., ROW_NUMBER, or grab a value from a specific other row, e.g., LAG.

##### 1.a. LAG/LEAD

For example: For each city, compute the ratio of population of the city vs. the next biggest city in the same state:

```sql
SELECT name, pop, CAST(pop AS float) / LAG(pop) OVER (PARTITION BY state ORDER BY pop)
FROM cities;
```

For LAG/LEAD with an offset of 1, the sum function will just remember the previous value if it exists, and `None` if it does not. (And we can have a similar one for LEAD.) This has the following properties:

- It's associative.
- It's not commutative, but that isn't a problem for Prefix Sum.
- The zero element is `None`. (`None` should be replaced by any `Some` value, and `Some` values should never be replaced by `None`.)

I built [a small prototype outside Materialize](https://github.com/ggevay/window-funcs), where I verified that the output values are correct, and that output is quickly updated for small input changes.

For LAG/LEAD with *k > 1* (which computes the given expression not for the previous record, but for the record that was *k* records ago), the sum function could simply remember the last *k* values, acting on a `Vec<Val>` of length at most *k*, which would generalize `Option<Val>`. This works kind of ok for small *k*. A more complicated but probably better solution is to find the index for that element in the window partition that is *k* elements behind by using the same method as we use for calculating the intervals of the framed window functions (see below). Then with the index in hand, we can just do a self-join.

##### 1.b. ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE

For example: List the two biggest cities of each state:
(Note that we can't directly write `ROW_NUMBER() <= 2`, because window functions are not allowed in WHERE clause.)

```sql
SELECT state, name
FROM (
  SELECT state, name, ROW_NUMBER()
    OVER (PARTITION BY state ORDER BY pop) as row_num
  FROM cities
)
WHERE row_num <= 2;
```

There is the **TopK** special case, i.e., where the user specifies `ROW_NUMBER() <= k` (or similar). We want to transform this pattern to our efficient TopK implementation, rather than using prefix sum. This should probably be an MIR transform. This way we can rely on MirScalarExpr canonicalization when detecting different variations of `rownum <= k`, e.g., `k >= rownum`, `rownum < k+1`, `rownum - 1 < k`.

In most situations other than TopK, these functions cannot be implemented efficiently in a streaming setting, because small input changes often lead to big output changes. However, as noted in the [Goals](#Goals) section, there are some special cases where small input changes will lead to small output changes. These will be possible to support efficiently by performing a Prefix Sum with an appropriate sum function.

#### 2. Window aggregations

These operate on frames (a certain subset of a window partition).
For example: Compute a moving average for each user's transaction costs on windows of 6 adjacent transactions:
```sql
SELECT user_id, tx_id, AVG(cost) OVER
(PARTITION BY user_id ORDER BY timestamp ASC
   ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
FROM transactions;
```

There is a special case where the frame includes the entire window partition: An aggregation where the frame is both UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING at the same time (or having no ORDER BY achieves a similar effect) should be transformed to a grouped aggregation + self join.

In all other cases, we’ll use prefix sum, for which we need to solve two tasks:

TODO: switch the order of I. and II.

*I.* We have to find the end(s) of the interval that is the frame. I.e., we need to tell **indexes** to Prefix Sum (where the index is a value of the ORDER BY column(s), as mentioned above).

*II.* We’ll need to generalize Prefix Sum to not just prefixes, but arbitrary intervals. (A prefix interval is identified by one index, a general interval is identified by two indexes.)

*Solving I. for each framing mode (RANGE | GROUPS | ROWS):*
- RANGE: this is the obvious one (but probably not the most often used): The offset is just a difference in the (sparse) “index” of the prefix sum (i.e., the ORDER BY column).
    - Btw. we might translate some inequality self-joins to this one!
- GROUPS: (One could say that this is probably not so often used, so no need to initially support it. However, the problem is that the solution for ROWS will probably build on this, and that is the default, so that one is often used.) We have to somehow translate the offset to a difference in the “index” of the prefix sum:
    - Naive solution: Since this is a difference in DENSE_RANK between the current row and the ends of the interval, we could compute a DENSE_RANK, and then join with that to find the “index” (the value of the ORDER BY column).
        - However, this would be slow, because DENSE_RANK is inherently not well incrementalizable: Small input changes can lead to lots of changes in the ranks.
    - **(Tricky part)** (TODO: this needs more details) A better solution is to calculate a count aggregation on all ranges (with prefix sum’s `aggregate`) (let's call this `counts`), and then do a logarithmic search for the index with a nested `iterate`:
        - We start with `(index, offset)` pairs for all the possible current elements in parallel, where the pair means that we need to move `index` down by `offset` (down when looking for the lower end of the interval, or up when looking for the upper end), i.e., we need to lower `index` while lowering `offset` to 0.
        - Then, at every step of an `iterate`, we can use a range from `counts` on each  `(index, offset)` pair: `index` is lowered by the size of the range, and `offset` is lowered by the aggregated count of the range.
            - We want to use the biggest such range in `counts` that doesn't make `offset` negative. We can do this by an inner `iterate`.
- ROWS: similar to GROUPS, but use indexes that include the hash. (see at [Peer groups](#Peer-groups))

There is also `frame_exclusion`, which sometimes necessitates special handling for the group that contains the current row. In such cases, we will put together the result value of the window function from 3 parts: 1. prefix sum (generalized to arbitrary intervals) for groups that are earlier than the current row’s group, 2. prefix sum for groups that are later than the current row’s group, 3. current row’s group (without prefix sum).

*Solving II.:*

For invertible aggregation functions (e.g., sum, but not min/max), it would be tempting to use the existing prefix sum with a minor trick: `agg(a,b) = agg(0,b) - agg(0,a)`. However, the performance of this wouldn't be good, because even if `(a,b)` is a small interval, the `(0,a)` and the `(0,b)` intervals will be big, so there will be many changes of the aggregates of these even for small input changes.

To have better performance (and to support non-invertible aggregations, e.g., min/max), we need to extend what the `broadcast` part of prefix sum is doing (`aggregate` can stay the same):
  - `queries` will contain intervals specified by two indexes.
  - `requests`: We can similarly compute a set of requests from `queries`. The change will only be inside the `flat_map`.
  - `full_ranges`, `zero_ranges`, `used_ranges` stay the same.
  - `init_states` won’t start at position 0, but at the lower end of the intervals in `queries`
  - The iteration at the end will be mostly the same.

#### 3. FIRST_VALUE / LAST_VALUE / NTH_VALUE

For example: For each city, compute how many times it is smaller than the biggest city in the same state:
```sql
SELECT state, name, pop,
       CAST(FIRST_VALUE(pop)
              OVER (PARTITION BY state ORDER BY pop DESC)
            AS float
       ) / pop
FROM cities;
```

These also operate based on a **frame**, similarly to window aggregations. (The above example query doesn't specify a frame, therefore it uses the default frame: from the beginning of the partition to the current row) They can be similarly implemented to window aggregations, i.e., we could “sum” up the relevant interval (that is not necessarily a prefix) with an appropriate sum function.

Alternatively, we could make these a bit faster (except for NTH_VALUE) if we just find the index of the relevant end of the interval (i.e., left end for FIRST_VALUE), and then self-join.

(And there are some special cases when we can transform away the window function usage: FIRST_VALUE with UNBOUNDED PRECEDING and LAST_VALUE with UNBOUNDED FOLLOWING should be transformed to just a (non-windowed) grouped aggregation + self-join instead of prefix sum trickery. Also, similarly for the case when there is no ORDER BY.)

----------------------

### Peer groups

There might be rows which agree on both the PARTITION BY key and the ORDER BY key (the index of the prefix sum). These groups of rows are called _peer groups_.

Framed window functions in GROUPS or RANGE frame mode as well as the RANK, DENSE_RANK, PERCENT_RANK, and CUME_DIST functions compute the same output for all the rows inside a peer group.
This is easy to handle, since we can simply deduplicate the indexes and compute one result for each index (and then join with the original data on the index). Note that the sums of each peer group (i.e., the input to `aggregate`) need to be precomputed by a standard grouped aggregation. (FIRST_VALUE and LAST_VALUE are handled specially as explained above.)

However, for framed window functions in ROWS frame mode as well as for LAG/LEAD and ROW_NUMBER we need to treat each element of a peer group separately. To make the output deterministic, we need to sort the rows inside a peer group by the entire row (as we do in all other sorting situations in Materialize). A straightforward way to achieve this would be to make all the components of the row part of the index of the prefix sum, but this is unfortunately impossible: First, [we will support only certain types in a prefix sum index](#ORDER-BY-types) (e.g., we don't support string), and second, recall that the bit length of the index is critical for the performance of prefix sum, so adding all columns to the index would be catastrophic for performance.

One idea to solve this problem is to have a `reduce` number the elements inside each peer group with 0, 1, 2, ... before prefix sum, and adding just this numbering column as an additional component to the prefix sum indexes. But the problem with this is that this would handle only small peer groups, since it's not incremental inside peer groups, i.e., it recomputes the numbering for an entire pee group when any group element changes. Even though, it might be ok in many cases to assume that peer groups are small (this should hold much more commonly than the assumption of the current window function implementation, which is that _window partitions_ are small), this would still hurt the generality of the whole prefix sum approach.

A better way to solve the problem is to fix a deterministic order of rows inside a peer group by _hashing the rows_, and making the hash part of the prefix sum index. The hashes will have an arbitrary, but deterministic order. The order being arbitrary doesn't matter, because the user didn't request any specific ordering on fields that don't appear in the ORDER BY clause.

Hash collisions will be resolved by an extra Reduce beforehand, which groups by hash value, and adds a few more bits (e.g., 8) to differentiate records within a collision group. If the collision resolution bits are not enough, i.e., there is a hash value that occurs more times than is representable by the collision resolution bits, then we error out.
Therefore, we'll have to determine the exact number of bits of the hash function's output as well as the number of collision resolution bits in a way that the chances of the collision resolution bits not being enough will be astronomically small for any realistically sized peer groups. My intuition is that 32 bits of hash + 8 bits of collision resolution are enough for peer groups of hundreds of millions, but [I'll make an exact calculation](https://oeis.org/A225871).

(Note that there is one common situation where peer groups are large: if there is no ORDER BY in the OVER clause. However, this particular case is not relevant here, since we are planning to handle this case by transforming away the window function to a grouped aggregation + a join, instead of using prefix sum.)

### ORDER BY types

Our prefix sum algorithm operates with indexes that are fixed-length bit vectors, which is a fundamental limitation of the algorithm. (The current implementation has `usize` hardcoded. We will generalize this to longer bit vectors, but they will still have to be fixed-length.) Therefore, any type that we would like to support in the ORDER BY clause of a window function executed by prefix sum will need to be mapped to fixed-length bit vectors. This unfortunately means that variable-length types, such as String, Array, List, Map, Bytes, won't be supported by prefix sum. For such types, we will fall back to the old, naive rendering (ideally, with a warning printed to the user, and possibly a Sentry log).

Fortunately, many important types _can_ be mapped to fixed-length integers, which we will discuss now. Importantly, the mapping should *preserve the ordering* of the type, i.e., if `a < b`, then `f(a) < f(b)` should also hold, where `f` is the mapping. Note that a type that is composed of a fixed number of fields of other types for which we already have a mapping can simply be mapped by concatenating the bits of the fields. This also allows us to support a composite ORDER BY key.

- Unsigned integer types (of any length) are fine.
- Signed integer types: we just need to fiddle with the sign to map them to an unsigned int in a way that preserves the ordering.
- Date is an i32.
- Time is two i32s.
- Timestamp types can be converted to Unix timestamps, which is an i64.
- MzTimestamp is an u64.
- Float types (surprisingly) [can also be supported](https://lemire.me/blog/2020/12/14/converting-floating-point-numbers-to-integers-while-preserving-order/).
- null can be handled by adding an extra bit at the beginning.
- Range is fine if the constituent types are supported, as it is basically two instances of an underlying type, plus various special values, for which a few extra bits have to be added.
- Uuid is simply 128 bits.

### Performance and optimizations

For the performance of `aggregate`, the size distribution of the *D_len* sets is important (see in the [prefix sum implementation section](#Implementation-details-of-DD's-prefix-sum)), since the implementation of `aggregate` performs one step for each _len_, and the time and memory requirements of each of these steps are proportional to the size of *D_len*. Critically, this means that large *D_len* sets (whose size is similar to the number of input elements) contribute a per-input-element overhead, while small *D_len* sets only contribute a per-partition overhead. We can compare this to the performance characteristics of a hierarchical aggregation: in that algorithm, the first several steps have almost the same size as the input, while the last few steps are small.

TODO: The following paragraph is not 100% correct.

So the question now is how quickly do the *D_len* sets get small as `aggregate` proceeds through its steps. Interestingly, the size distribution of the `D_len` sets depends on where are the variations in the bit vectors of the indexes in the input. For example, if all indexes start with 10 zero bits, then the last 10 *D_len* sets each will have only one element, and thus the last 10 steps of `aggregate` will contribute only a per-partition overhead. However, if all indexes _end_ with 10 zero bits, then each of the _first_ 10 *D_len* sets will have a similar size as the input data.

TODO: mention somewhere that we won't put entire rows inside the prefix sum, but only the index + the value of the expression appearing in the window function.

Optimizations: TODO:

#### Special rendering for LAG/LEAD

Instead of prefix sum, ...

Performance would be similar to a 16-stage hierarchical aggregation

#### Dynamically switch to old implementation for small window partitions

to eliminate the problem of the huge per-partition overhead (memory and time)

#### Each `aggregate` step should step several bits instead of just 1 bit

This would reduce the time overhead of `aggregate`. It would also reduce the memory overhead of `aagregate` by reducing the memory need of the internal operations, but it wouldn't reduce the total output size of `aggregate`.

# Alternatives

I will now discuss alternatives to various aspects of the design.

## Not supporting window functions

An easy way out would be to not support window functions at all. This alternative was seriously considered before, because supporting window functions seemed very hard, if not impossible. However, it turned out that [many users are requesting window function support](https://www.notion.so/materialize/Window-Functions-Use-Cases-6ad1846a7da942dc8fa28997d9c220dd). Also, we came up with execution approaches that make supporting window functions feasible. Therefore, I think it is now clear that we should support window functions.

## Staying with the current implementation

We already have an implementation for several window functions. However, one of the main goals of Materialize is to be scalable, and our current implementation becomes extremely inefficient with large window partitions. This is because it recomputes results for an entire window partition even when just one element changes in the partition. This behavior breaks Materialize's core promise of reacting to small input changes with a small amount of computation. The issue is already quite severe with just a few hundred elements in window partitions, therefore users would run into this limitation quite often.

The implementation suggested in this document would be scalable to huge window partition sizes. It parallelizes computations even inside a single partition, and therefore even partitions larger than one machine will be supported.

## Rendering alternatives

The main proposal of this document is to use DD's prefix sum (with extensions/generalizations) for the efficient rendering of window functions, but there are some alternatives to this.

### Custom datastructures instead of prefix sum

Instead of relying on DD operators to implement prefix sum, we could create a new DD operator specifically for window functions. This new operator would have a specialized data structure inside, e.g., something like a BTreeMap (with some extra info precomputed for the subtree under each node for certain window functions). When an input element is added/removed, we find it in the data structure in *log p* time, where _p_ is the partition size, traverse the neighborhood of the changed element and update the data structure and the output. This might have a serious efficiency advantage over relying on DD's prefix sum.

Some examples:
- LAG/LEAD with offset 1 is trivial: we find the changed element (in *log p* time), check just the previous and next element and update the output appropriately.
- LAG/LEAD with offset _k_: we store the number of elements in each node, and then we are able to navigate _k_ elements back or forward in *log k* time (after finding the changed element in *log p* time). (We need two navigations from the changed element to collect the necessary info for updating the output: _k_ elements backward and _k_ elements forward.)
- LAG/LEAD with IGNORE NULLS: We additionally store the number of non-null elements in the subtree, and then we are able to navigate from the changed element to the target element in *log d* time, where _d_ is the number of rows that we would step from the changed element to the target element if we were to be stepping one-by-one.
- Window aggregations with a frame size of _k_ with ROWS frame mode: We find the changed element in *log p* time, and then we gather info from and update the previous and next _k_ elements, and emit _~2k_ updates. GROUPS frame mode is similar, but more updates and emissions are needed. The number of updates and emissions would still be approximately equal to each other. RANGE mode is also similar, but instead of stepping _k_ elements, we step until we reach a sufficient offset in the ORDER BY key. The number of steps would be similar to the number of updated output elements here too.
- Window functions with an UNBOUNDED PRECEDING frame: We simply store the actual prefix sum for each element, in addition to the tree data structure. (No need to put it inside the tree, can be a separate BTreeMap.) The number of elements that need to be updated in this data structure is the same as the number of output elements that are changing.
- FIRST_VALUE/LAST_VALUE with a frame size of _k_: Each tree node should store the number of elements in its subtree. We find the updated element in *log p* steps, and then find the target value in *log k* steps.

Pros:
- Efficiency, due to several reasons:
  - Computation times and memory requirements here don't involve the bit length of the input indexes (_b_). Instead, the logarithm of the partition sizes (_p_) is involved. Having _log p_ instead of _log b_ can often be a factor of several times.
  - The above _log p_ is hidden inside sequential code (inside the logic of a DD operator instead of calling DD operators), while in the case of prefix sum, there are _log b_ DD operators chained together. This means that in the case of prefix sum, the _log b_ is _multiplied_ by an extra logarithmic factor that comes from manipulating arrangements as part of each DD operation.
- No need for certain complications: a mapping of various types to integers, handling peer groups by hashing, [complicated optimizations](#Performance-and-optimizations).
- Would work with arbitrary types in the ORDER BY (e.g., string). TODO: make these links to sections

Cons:
- Partition sizes would not be scalable beyond a single machine, since each partition is stored in a single instance of the data structure. (Contrast this with prefix sum implemented by DD's data-parallel operations.)
- This approach wouldn't compose nicely with WITH MUTUALLY RECURSIVE. DD's prefix sum would be incremental not just with respect to changing input data, but also with respect to changes from one iteration of a recursive query to the next. This is because DD's operations and data structures (arrangements) are written in a way to incrementalize across Timely's complex timestamps (where the timestamps involve source timestamps as well as iteration numbers). Our custom data structure would incrementalize only across source timestamps, by simply updating it in-place when a source timestamp closes. But between each iteration, it would need to be fully rebuilt.
- Not supporting partially ordered timestamps might be problematic not just for WMR. There are some other plans to use complex timestamps, e.g., for higher throughput read-write transactions. Further uses for partially ordered timestamps might be discovered later.
- Writing a custom DD operator is very hard.
- A more philosophical argument is that if we implement the more general approach first (prefix sum), then we can gather data on how people really want to use window functions (whether they want huge partitions that don't fit on 1 machine, whether they want to use it in WMR), and then possibly implement various optimizations (maybe even custom datastructures) for popular cases at some future time with less guesswork.

An interesting option would be to allow the user to switch between prefix sum and a custom datastructure rendering. This could be realized by a new keyword after the PARTITION BY clause, e.g., SINGLEMACHINE.

### Implement Prefix Sum in MIR (involving `LetRec`) instead of a large chunk of custom rendering code

The main plan for implementing Prefix Sum is to implement it directly on DD (and represent it as one node in LIR). An alternative would be to implement Prefix Sum on MIR: Prefix Sum's internal joins, reduces, iterations, etc. would be constructed not by directly calling DD functions in the rendering, but by MIR joins, MIR reduces, MIR LetRec, etc. In this case, the window function handling code would mainly operate in the HIR-to-MIR lowering: it would translate HIR's WindowExpr to MirRelationExpr.

Critically, the Prefix Sum algorithm involves iteration (with a data-dependent number of steps). Iteration is possible to express in MIR using `LetRec`, which is our infrastructure for WITH MUTUALLY RECURSIVE. However, [this infrastructure is just currently being built, and is in an experimental state at the moment](https://github.com/MaterializeInc/materialize/issues/17012). For example, the optimizer currently mostly skips the recursive parts of queries, leaving them unoptimized. This is a long way from the robust optimization that would be needed to support such a highly complex algorithm as our Prefix Sum. Therefore, I would not tie the success of the window function effort to `LetRec` at this time.

Still, at some future time when we are confident in our optimizer's ability to robustly handle `LetRec`, we might revisit this decision. I'll list some pro and contra arguments for implementing Prefix Sum in MIR, putting aside the above immaturity of `LetRec`:

Pros:
- Prefix Sum could potentially benefit from later performance improvements from an evolving optimizer or rendering.
- We wouldn't need to specially implement certain optimizations for window functions, but would instead get them for free from the standard MIR optimizations. For example, [projection pushdown through window functions](https://github.com/MaterializeInc/materialize/issues/17522).
- Optimization decisions for Prefix Sum would be integrated with optimizing other parts of the query.

Cons:
- Creating MIR nodes is more cumbersome than calling DD functions. (column references by position instead of Rust variables, etc.)
- We would need to add several scalar functions for integer bit manipulations, e.g., for extracting set bits from integers.
- Computing the scalar expressions would be much slower as long as we don't have [vectorization for them](https://github.com/MaterializeInc/materialize/issues/14513).
- When directly writing DD code, we have access to all the power of DD, potentially enabling access to better performance than through the compiler pipeline from MIR.

## Where to put the idiom recognition?

This document proposes recognizing the windowing idiom (that the HIR-to-MIR lowering creates) in the MIR-to-LIR lowering. An alternative would be to do the idiom recognition in the rendering. In my opinion, the lowering is a more natural place for it, because:
- We shouldn't have conditional code in the rendering, and this idiom recognition will be a giant piece of conditional code.
- We want (at least) EXPLAIN PHYSICAL PLAN to show how we'll execute a window function.
- We need to know the type of the ORDER BY columns, which we don't know in LIR. (Although we could add extra type info just to window function calls during the MIR-to-LIR lowering to get around this issue.)

## Representing window functions in each of the IRs

Instead of recognizing the HIR-to-MIR lowering's window functions idiom during the MIR-to-LIR lowering, we could have an explicit representation of window functions in MIR. More generally, there are several options for how to represent window functions in each of HIR, MIR, and LIR. For each of the IRs, I can see 3 options:

1. Create a new relation expression enum variant. This could be a dedicated variant just for window functions, or it could be a many-to-many Reduce, which would initially only handle window functions, but later we could also merge `TopK` into it. (Standard Reduce is N-to-1, TopK is N-to-K, a window function is N-to-N. There are differences also in output columns.)
2. Hide away window functions in scalar expressions. (the current way in HIR)
3. Reuse an existing relation expression enum variant, e.g., `Reduce`.

### HIR

In HIR, the window functions are currently in the scalar expressions (option 2. from above), but it’s possible to change this.

1. *Dedicated `HirRelationExpr` variant:*
    - There is a precedent for a similar situation: HIR has aggregation expressions, which (similarly to window expressions) have the property that they are in a scalar expression position, but their value is actually calculated by a dedicated `HirRelationExpr` variant (`Reduce`), and then there is just a column reference in `HirScalarExpr`.
2. *Hiding in `HirScalarExpr`:*
    - This makes sense to me, because HIR wants to be close to the SQL syntax, and window functions appear in scalar position in SQL.
    - It’s already implemented this way, so if there is no strong argument for 1. or 3., then I’d like to just leave it as it is.
3. (*Reusing `Reduce`*. In MIR and LIR this option can be considered, but I wouldn't want Reduce to get complicated already in HIR.)

### MIR

The current plan is to *not* have an explicit representation of window functions in MIR for now. Here, we first discuss some reasons for this. Then, we discuss how such a representation could look like, as it is still on the table for a future evolution of window function handling, when
- the currently ongoing optimizer refactoring is completed;
- we have data indicating that the pattern recognition is indeed too brittle.

We decided not to have an explicit representation because it would mean that we would have to immediately teach all existing transforms how to handle window functions, which would be a lot of code to write. Current transforms at least don't do incorrect things with window functions. (However, some transforms might currently not be able to do their thing on the complicated pattern that the HIR lowering creates for window functions, for example [projection pushdown doesn't work for window functions](https://github.com/MaterializeInc/materialize/issues/17522).)

I'm hoping that MIR transforms won't create too many variations of the window function pattern that HIR lowering creates. The pattern involves FlatMap, which not many transforms actually manipulate much. Also, the data is in a very weird form during the pattern (the whole row hidden inside a (nested) record), so some transforms cannot handle that, e.g. ProjectionPushdown.

Having an explicit MIR representation instead of the current pattern would also mean that we would have to put in extra work to allow falling back to the old window function rendering code in such cases that are not yet covered by the prefix sum rendering. More specifically, we would need to port the code that creates the pattern from the HIR lowering to an MIR transformation (or to MIR lowering). This code would create the old pattern from the explicit MIR representation instead of from the HIR representation.

Note that the above problem of porting the pattern creation code would disappear if we were to port _all_ window functions to the prefix sum rendering, because then all need for the old pattern would disappear with an explicit MIR representation. However, prefix sum doesn't handle types that cannot be mapped to a fixed-length integer types (e.g., string), for which the old rendering will be needed as a fallback. This means that, unfortunately, we might never be able to remove the old rendering.

The 3 representation options in MIR are:

*1. Create a dedicated enum variant in `MirRelationExpr`*

I think this is better than 2., because Map (and MirScalarExprs in general) should have the semantics that they can be evaluated by just looking at one input element, while a window function needs to look at large parts of a window partition. If we were to put window functions into scalar expressions, then we would need to check lots of existing code that is processing MirScalarExprs that they are not getting unpleasantly surprised by window functions.

Compared to 3., it might be easier to skip window functions in many transforms. This is both good and bad:
  - We can get a first version done more quickly. (And then potentially add optimizations later.)
  - But we might leave some easy optimization opportunities on the table, which would come from already-existing transform code for `Reduce`.

A new `MirRelationExpr` variant would mean we have to modify about 12-14 transforms `LetRec` is pattern-matched in 12 files in the `transform` crate, `TopK` 14 times. See also [the recent LetRec addition](https://github.com/MaterializeInc/materialize/commit/9ac8e060d82487752ba28c42f7b146ff9f730ca3) for an example of how it looks when we add a new `MirRelationExpr` variant. (But note that, currently, LetRec is disabled in all but a few transforms)

When considering sharing a new many-to-many Reduce variant between window functions and TopK, we should keep in mind that the output columns are different: TopK keeps exactly the existing columns, but a window function adds an extra column.

*(2. Hiding window functions in `MirScalarExpr`*)

This seems scary to me, because scalar expressions should generally produce exactly one value by looking at exactly one record, which is not true for window functions. It's hard to tell that none of the code that is dealing with scalar expressions would suddenly break.

Also note that `MirScalarExpr` can occur in several places (`JoinClosure`, etc.), so we would have to attend to window functions in the lowerings of each of these.

*3. We could consider putting window functions in `MirRelationExpr::Reduce`.* This was suggested by Frank: [https://materializeinc.slack.com/archives/C02PPB50ZHS/p1672685549326199?thread_ts=1671723908.657539&cid=C02PPB50ZHS](https://materializeinc.slack.com/archives/C02PPB50ZHS/p1672685549326199?thread_ts=1671723908.657539&cid=C02PPB50ZHS)

`Reduce` is pattern-matched in 20 files in the `transform` crate. All of these will have to be modified. This is a bit more than the ~12-14 pattern matches of adding a new enum variant, because there are some transforms specialized to `Reduce`, which we wouldn't need to touch if it were a new enum variant instead.

The main argument for this is that maybe we could reuse some of the code that is handling `Reduce`. However, there are two big differences between grouped aggregation and window functions, which hinders code re-use in most places:
  1. The output columns are different: A grouped aggregation’s output is the grouping key columns and then one column for each aggregate, but a window function retains all columns, and then just appends one column at the end (regardless of the grouping key).
  2. Grouped aggregation produces exactly one row per group, while window functions produce exactly one row per input row. To solve this difference, Frank is saying we could generalize `Reduce`, making it many-to-many, [as in DD's `reduce`](https://github.com/TimelyDataflow/differential-dataflow/blob/master/src/operators/reduce.rs#L71). (To me, it seems that matching up MIR Reduce's behavior with DD’s Reduce would be useful if the translation of MIR's Reduce would be to just call DD’s Reduce, but this is not the case at all for window functions.)

It seems to me that the overlap between the current `Reduce` handling and how to handle window functions is not big enough to justify putting the window functions into `Reduce`. There would be ifs every time we handle `Reduce`, and different things would be happening for traditional `Reduce` and window function `Reduce`. We could later have a separate EPIC to consider unifying window function and Reduce (and potentially TopK) into a many-to-many “super-reduce”, as this seems to be a separate work item from window functions.

Examining code reuse possibilities for some example transformations:
- `ColumnKnowledge`
    - The `optimize` call for the `group_key` could be reused (for the key of the PARTITION BY), but this is just a few lines.
        - But they cannot be pushed to the `output` `Vec<DatumKnowledge>`, because the grouping key columns are not part of the output. Instead, the knowledge from the original columns should be pushed.
    - The rest of the code is also somewhat similar to what needs to happen with window functions, but not exactly the same, due to the more complicated representation of window expressions (`WindowExprType`) vs. aggregate expressions. (`AggregateExpr`). So, it seems to me that code sharing wouldn't really help here.
- `FoldConstants`: The heavy lifting here is in `fold_reduce_constant`, which is completely different from what is needed for window functions. The rest of the code is similar, but not identical.
- `JoinImplementation`: This tries to reuse arrangements after a Reduce, which we cannot do for window functions. So we would have to special-case those Reduces that are actually window functions.
- `MonotonicFlag` is easy either way.
- `ReduceElision` could be partially re-used, but we would need to add some ifs due to the different output columns. Also, we would have to implement a new `on_unique` for window functions. (Although, this one doesn't sound like a terribly useful optimization for window functions, because it’s hard to see how a window function call could end up on a unique column.)
- `LiteralLifting`: The two inlinings at the beginning could be reused. (But those are already copy-pasted ~5 times, so they should rather be factored out into a function, and then they could be called when processing a new enum variant for window functions.) The rest probably not so much.
- ...

### LIR

1. We could add a dedicated LIR `Plan` enum variant. This sounds like the right approach to me, because window functions will have a pretty specific rendering (Prefix Sum) that’s distinct from all other operators, and LIR is supposed to be a close representation of what we are about to render.
2. (We can pretty much rule out hiding them in scalar expressions at this point, because scalar expressions get absorbed into operators in all kinds of ways, and we don't want to separately handle window functions all over the place.)
3. (Another option is to model it as a variant of `Reduce`. But I don’t see any advantage of this over an own enum variant. I don’t see any code reuse possibility between the existing `Reduce` rendering and the rendering of window functions by Prefix Sum.)

# Rollout

## Testing

### Correctness

Since there is already some window functions support (it’s just inefficient), there is already `window_funcs.slt` (it's a lot of tests, 4845 lines). However, some window functions and window aggregations (and some options, e.g., IGNORE NULLS, some tricky frames) are not supported at all currently, so those are not covered. I’ll add tests to this file for these as well. I will also add more tests that will cover the interesting corner cases of the new rendering specifically.

Additionally, there is `cockroach/window.slt` (3140 lines), which is currently disabled (with a `halt` at the top of the file). We’ll re-enable this, when our window function support will be (nearly) complete.

Fuzzing would be great, because there is a large number of window functions and frames and options, so it's hard to cover all combinations with manually written queries. Also, in randomly generated queries we could add many other things around window functions, thus testing that window functions work in various contexts.

We'll need to extensively test the idiom recognition that recognizes the pattern that the HIR lowering creates for window functions. Fortunately, the first step of the idiom recognition is quite easy: we just need to look at the aggregate function of the `Reduce`, and decide if it is a window function. Then, if this first step finds a window function, we could soft-assert that we manage to recognize all the other parts of the pattern. This way, all the above tests would be testing also the idiom recognition (even when a test doesn't involve EXPLAIN).

Also, there could be a sentry warning with the above soft assert, so that we know if a customer is probably running into a performance problem due to falling back to the old window function implementation.

(If we were to reuse `MirRelationExpr::Reduce` to represent window functions, then we’ll have to pay extra attention that existing transforms dealing with `Reduce` are not messing up window functions.)

### Performance

Importantly, we need to test that we efficiently support situations when small input changes lead to small output changes.

Writing automated performance tests is tricky though. We have not yet developed the infrastructure for it, as we currently don’t have any automated performance tests.

At least manual testing should definitely be performed before merging the PRs, since the whole point of this work is performance. We could do it roughly as follows: We put in lots of input data with one timestamp, as an “initial snapshot”. Processing this should be at least several seconds. Then, we change a small portion of the input data. Importantly, even though the input data change is small, the total size of the affected partitions should cover most of the input data. This is needed for this test because the current window function support works fine for small window partitions. Processing the input data changes should complete orders of magnitude faster than the initial snapshot.

An automated way to implement the above could be as follows (say, in Testdrive):
1. Copy some TPC-H data from our TPC-H source into tables.
2. Create a materialized view with some window functions on the tables.
3. Do some inserts/updates/deletes on the tables.
4. Check that updating of the materialized view happens quickly. It should be possible to set up the input data and the queries in such a way that updating takes orders of magnitude faster than reacting to the initial snapshot. (But not with the current window function support.) The difference from the initial snapshot should be big enough so that the test won’t be flaky. (I already tested a [simple prototype for LAG outside Materialize](https://github.com/ggevay/window-funcs).)

We should also measure the memory requirements of our new rendering, so that we can advise users on sizing replicas.

## Lifecycle

There are many window functions, and many frame options. We will gradually add the new, efficient implementations for an increasing set of window function + frame setting combinations across several PRs. I created a breakdown into EPICs and lower-level issues [here](https://github.com/MaterializeInc/materialize/issues/16367). We will make sure the new implementation is correct and performs well before merging each of the PRs.

# Open questions

Do we have enough arguments for choosing prefix sum over custom data structures? Or maybe we could implement the custom datastructure rendering approach first, and later implement the prefix sum approach, and then give the option to the user to switch to the prefix sum rendering? (See the "Custom datastructures instead of prefix sum" section.)

We should check that there is correct parallelization inside window partitions, see above.

How to have automated performance tests? How can we check in Testdrive that some materialized view (that has window functions) is being updated fast enough? (This is not critical for the first version; we'll use manual performance tests.)
