# Understanding what Costs in Materialize

Materialize builds dataflow graphs that represents queries.
The operators in these graphs can either be *stateless* (as in a Map, Filter, or Project operator) or *stateful*.
Stateful operators hold on to arrangements of records, which are the main contributor to memory footprint in Materialize.
Arrangements can be shared between operators that need the same data, which can ameliorate some of the cost.

Let's talk through what an arrangement is, which operators require arrangements, and what we can do about them.

## What is an "Arrangement"?

An arrangement is an alternative representation of a stream of update triples `(data, time, diff)`.
In stream form, the triple pass by and are gone.
An arrangement records these triples, and indexes them by `data`.
With some caveats about "logical compaction", the index maintains the *current* accumulation of `diff` for each `(data, time)`.
This means that the size of an arrangement should be roughly the current size of the accumulated updates, where "zero" accumulations contribute nothing.
An arrangement undergoes "physical compaction" lazily, which means that the actual size of an arrangement can exceed the logical size, for what should be a bounded amount of time.

An arrangement only costs proportional to the number of distinct `(data, time)` pairs.
This number can be small even if the number of records is large, if we were able to reduce `data` to few values.
For example, if we want to histogram taxi rides by number of occupants and fare amount, there may be few distinct pairs despite many records.

Arrangements are actually indexed assuming that `data` has a `(key, val)` structure.
The index is by `key`, which will be important in understanding when arrangements can be shared and reused.
Materialize will attempt to choose a `key` to ensure that the data are well distributed.
For example, if a collection has a primary key, Materialize will default to this key for an arrangement.
Each of `key` and `val` can be `()`, corresponding to "no key" and "the key is the value", respectively.

Materialize only stores each column once in an arrangement.
If a relation is indexed by a subset of its columns, the key columns will be de-duplicated from the value, such that the value only contains non-key columns.
This avoids a memory overhead otherwise incurred by storing the same data multiple times.

To investigate the existing arrangements, query the `mz_arrangement_sizes` logging source.
There are several diagnostic views in `mz_catalog` that connect this information to operator names, and group it by dataflow.

## Which operators need Arrangements?

We create more arrangements than just those to house indexes and materialized views.
Many internal operators require arrangements so that they can respond efficiently to changes.
For example, the differential dataflow `join` operator requires that each of its two inputs be an arrangement.
Specifically, each arrangement must have as its `key` the fields that will be equated by the join.
Additionally, the differential dataflow `reduce` operator requires that both its input and its output be an arrangement.
These two operators are at the heart of the arrangement use in Materialize, because we construct relational operators out of them.

Materialize has several relational operators that are "rendered" using differential `join` and `reduce` operators.
These are the operators you would see in `explain plan for <query>`.
There is a more complicated mapping from SQL to these operators.

---

Our `Join` operator joins together a sequence of input relations.
It creates a sequence of differential `join` operators, each of which need to have both inputs be arrangements.
This means that in addition to arranging each of the join inputs, it arranges each of the intermediate results.
For example, in the figure below, each `A` is an arrangement.

```
    In2 --> A  In3 --> A  In4 --> A
             \          \          \
    In1 --> A-Join --> A-Join --> A-Join --> Out
```

To join four inputs together, there are six arrangements.
Some of these input arrangements could be shared, but most likely the intermediate arrangements cannot.

---

Our `Reduce` operator builds a non-trivial dataflow subgraph. In the subgraph,
arrangements always come in input-output pairs.

As of v0.7.1, the dataflow subgraph depends on 1) what types of aggregations are
involved and 2) the type of input for the reduce.

### Distinct (no aggregations)

Creates a pair of arrangements, each with row count `<number of groups>` and
row width `<width of key>`.

```
In -> InA -> OutA -> Out
```

### Only accumulable aggregations (sum/counts/any/all):

Creates a pair of arrangements, each with row count `<number of groups>` and
row width `<width of key> + <width of aggregation columns>`.

```
In -> InA -> OutA -> Out
```

### Only hierarchical aggregations (min/max) on an append-only input:

Same as the "Only accumulable aggregations" case above.

### Only hierarchical aggregations (min/max) on other inputs:

Hierarchical aggregations on inputs that are not append-only have
`ceil(log_16(expected_group_size))` stages. If an
`expected_group_size` has not been supplied via query hint, the default value
is 4 billion.

In the `n`th to last stage, the rows belonging to the same group are hashed
across `16^(n-1)` subgroups. The top value from each subgroup advances to the
`n-1`th to last stage.

Thus, for each group, the input arrangement of the `n`th to last stage will
contain `min(16^n, <size of group>)` rows. The exception is the first stage,
which will always contain all rows from the input.

The output arrangement of the final stage is supposed to contain the top row
for each group. For all other stages, the output arrangement contains the rows
in the input arrangement of that stage that are not in the input arrangement of
the next stage.

A two-stage dataflow would look like this:
```
In -> PrevStgInA -> PrevStgOutA -> Subtract -> FinStgInA -> FinStgOutA -> Out
           \                           /
            -------------------------->
```

The row width for all arrangements in this case are
`<width of key> + <width of aggregation columns>`.

A general heuristic for the total row count of all the arrangements, assuming a
well-chosen `expected_group_size`, is
`(2 + epsilon) * <number of distinct rows>`.
The input arrangement of the first stage will always contain all rows from the
input. `<row count of the input arrangement of the second stage> + `
`<row count of the output arrangement of the first stage>` is always equal to
`<row count of the input arrangement of the first stage>`.

For best performance, we recommend always specifying an accurate
`expected_group_size` if you have this kind of aggregation.

Specifying an `expected_group_size` that is magnitudes larger than the actual
sizes of each group will result in there being unnecessary stages that don't
reduce the number of candidate rows at all, resulting in the arrangements having
a total row count of `> 3 * <number of distinct rows>`.

Specifying an `expected_group_size` that is magnitudes smaller than the actual
sizes of each group may result in increased query latency for some input
streams. We recalculate the top row in a subgroup whenever the subgroup receives
an update. Thus, having too many rows in a subgroup will result in more
frequent and more expensive recalculating effort.

### Other aggregations (jsonb_agg + others):

A pair of arrangements are produced for each aggregation of type "other".
The input arrangement has row count `<number of distinct rows>` and row width
`<width of key> + <width of column being aggregated>`. The output arrangement
has row count `<number of groups>` and row width
`<width of key> + <width of aggregation columns>`

```
In -> InA -> OutA -> Out
```

If there are multiple aggregations of type "other", then there will be a pair of
arrangements collating the results of the type "other" aggregations. The input
collation arrangement size equal to the concatenation of the
output arrangements for each aggregation, except that there is an extra byte on
every row to mark which aggregation the row came from. The output collation
arrangement has row count `<number of groups>` and row width
`<width of key> + <width of aggregation columns>`.

```
In -> <Other agg 1 branch > -> Concat -> CollationInA -> CollationOutA -> Out
  \                              /
   < Other agg 2 branch > ------>
```

### Aggregations on distinct values

`Min` and `Max` aggregations on distinct values are computed the same as `min`
and `max` aggregations on non-distinct values because they have the same
result.

Each other aggregation on distinct values requires an additional
pair of arrangements on top of the arrangements required for the particular
aggregation type. The
arrangements have row count `<count(distinct <key + aggregation columns>)>`
and row width `<width of key> + <width of aggregation columns>`.

For example, if we had
```
select sum(distinct col1), sum(distinct col2), sum(col3)
from t
group by key
```
there would be a pair of arrangements for the fact that all aggregations are
accumulable. There would be two more pairs of arrangements for the fact that two
of the aggregations are on distinct values.

```
In -> Distinct1InA -> Distinct1OutA -> Concat -> AccumulableInA -> AccumulableOutA -> Out
 \ \                                /   /
  \ Distinct2InA -> Distinct2OutA ->   /
   \                                  /
    --------------------------------->
```

### Combination of different types of aggregations

The dataflow splits into `n` branches, where `n` is the number of different
types of aggregations present. A pair of arrangements that collate the results
so all the aggregations show up in one row.

```
In -> <Accumulable agg branch> ---> Concat -> CollationInA -> CollationOutA -> Out
 \  \                               /   /
  \   <Hierarchical agg branch> -->    /
   \                                  /
    <Other agg branch> -------------->
```

The input collation arrangement is the same size as the concatenation of the last
arrangement of every branch, except that there is an extra byte on every row to
mark which branch the row came from.

The output collation arrangement has row count `<number of groups>` and row
width `<width of key> + <width of all aggregations>`.

---

Our `TopK` operator builds a sequence of 16 `reduce` operators.
Each `reduce` operator applies the `TopK` logic over progressively coarser groups
The final group is just by the group key.

---

Our `Threshold` operator creates one `reduce` operator.

---

Our `ArrangeBy` operator creates one arrangement.

---

For each of these relational operators, the associated memory footprint should determined by the sizes of the arranged collections.

## Caveats: Shared Arrangements

Arrangements may be shared between operators in the same dataflow, or across dataflows if an arrangement is published by the dataflow that creates it.
Arrangements are most commonly published by the creation of indexes and materialized views.
Materialize does not currently publish other arrangements.

The `Join` operator requires arrangements each with `key` equal to the columns that will be equated.
When joining with a collection that has a primary key, we may have that arrangement available.
In this case, we are able to re-use the existing arrangement, and incur no additional memory use for this arrangement.

The `Reduce` operator concludes with an arrangement of its output, whose `key` is the grouping key.
It is not uncommon to re-use this arrangement, as we find that such groupings are often followed by joins.

The `mz_arrangement_sharing` logging source reports the number of times each arrangement is shared.
An arrangement is identified by the worker and operator that created it.

## Caveats: Delta Joins

In certain circumstances, we plan `Join` operators using a different pattern which avoids the intermediate arrangements.
The alternate plan uses more arrangements, but the arrangements are more likely to be shareable with other queries.
Informally, if all collections have arrangements by all of their primary and foreign keys, we can apply the Delta Join plan.

The plan removes the requirement that the intermediate results be arranged, but requires a separate dataflow for each input.
For example, we would be able to write the following dataflow to handle changes to `In1`:
```
    In2 --> A  In3 --> A  In4 --> A
             \          \          \
    In1 ----> Join ----> Join ----> Join --> Out
```
But, we would also need to create similar dataflow graphs for each of `In2`, `In3`, and `In4`.
In each case they only require arrangements of the input, but they may be by different keys.

If a `Join` is implemented by a Delta Join pattern, it will create zero additional arrangements.

## Caveats: Demand Analysis

> Obsolete for `Join` and `FlatMap` as of v0.9.4. We now delete fields that are
> not required instead of blanking them out. Plans prior to v0.9.4 will show
> something like `| | demand = (#6, #8, #12, #15, #22, #23, #27)` for the
> `Join` and `FlatMap` operators, listing which field will be blanked out.

When users present queries and views to us, we can determine that some fields
are not required and blank out them out. This can reduce the number of distinct
`data` in an arrangement, which will reduce the size of the arrangement.
Currently, we blank out fields not required when importing sources.
