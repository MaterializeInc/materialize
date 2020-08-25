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
With some caveats about "logical compaction", the index maintains the *current* accumulation for each `(data, time)`.
This means that the size of an arrangement should be roughly the current size of the accumulated updates, where "zero" accumulations contribute nothing.
An arrangement undergoes "physical compaction" lazily, which means that the actual size of an arrangement can exceed the logical size, for what should be a bounded amount of time.

An arrangement only costs proportional to the number of distinct `(data, time)` pairs.
This number can be small even if the number of records is large, if we were able to reduce `data` to few values.

To investigate the existing arrangements, query the `mz_arrangement_sizes` logging source.
There are several diagnostic views in `mz_catalog` that connect this information to operator names, and group it by dataflow.

## Which operators need Arrangements?

We create more arrangements that just those to house materialized sources and views.
Many internal operators require arrangements so that they can respond efficiently to changes.
For example, the differential dataflow `join` operator requires that each of its two inputs be an arrangement.
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
    In1 --> A-Join --> A Join --> A-Join --> Out
```

To join four inputs together, there are six arrangements.
Some of these input arrangements could be shared, but most likely the intermediate arrangements cannot.

---

Our `Reduce` operator builds a non-trivial dataflow subgraph (which may be subject to change).
Informally, for aggregation we build its own dataflow.
That dataflow may begin with a `reduce` operator to implement the `distinct` keyword.
Then there is either
    * a single `reduce` operator (for sums, counts, any, all), or
    * a sequence of 16 `reduce` operators (for min and max), or
    * a single `reduce` operator (for `jsonbagg` and others).
Finally, last `reduce` collects each of the aggregates into one record for each key.
There are special cases when there are few (zero or one) reductions.

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

Arrangements may be shared between operators in the same dataflow, or across dataflows for published arrangements.

In particular, for `Join` it is often the case that the join will be "on" columns that form a primary key for the relation, and for which we have built an arrangement. In this case, we are able to re-use the existing arrangement, and incur no additional memory use for this arrangement.

The `Reduce` operator concludes with an arrangement of its output, and it is not uncommon to be able to re-use this arrangement.

The `mz_arrangement_sharing` logging source reports the number of times each arrangement is shared.
An arrangement is identified by the worker and operator that created it.

## Caveats: Demand Analysis

When users present queries and views to us, we can determine that some fields are not required.
We blank out any field that is not required.
This can reduce the number of distinct `data` in an arrangement, which will reduce the size of the arrangement.
