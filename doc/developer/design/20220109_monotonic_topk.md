# Monotonic TopK Rendering: Current State and Potential Improvements

Created at: December 16, 2022 5:35 PM;
GitHub epic: https://github.com/MaterializeInc/database-issues/issues/4838

## Summary
This document aims at capturing a high-level understanding of `TopK` rendering code as well as propose potential improvements in rendering monotonic top-k plans with strategies that that are more efficient when k is small, as is common in practice.

## Goals
Presently, our rendering of `TopK` plans treats three cases: `MonotonicTop1Plan`, `MonotonicTopKPlan`, and `BasicTopKPlan`. Taking a step back, there seem to be at least two aspects to consider in our rendering options: monotonicity and boundedness. Monotonicity is an input property that allows us to come up with more specialized rendering for `TopK` plans, since we do not have to account for retractions and can thus not maintain as much state. Boundedness can refer to the input or to the output sizes. It has been observed in issue [#14444](https://github.com/MaterializeInc/database-issues/issues/4127) that input size boundedness can be exploited to simplify `TopK` chains. Output size boundedness is exploited in the rendering of `MonotonicTop1Plan` as well as `MonotonicTopKPlan`. However, there may be opportunities to improve the rendering even further, especially for small k, of `MonotonicTopKPlan`.

## Non-Goals

We do not aim at providing improvements to `MonotonicTop1Plan` nor to `BasicTopKPlan` at this point. However, analysis contained herein may prove informative for future efforts aimed at these other rendering flavors.


## Description: Analysis of Current State of Affairs

We provide below an overview and discuss general observations about the present rendering strategies for `TopK` plans. For simplicity, the description here ignores group-by keys, but extension of the strategies to also include this additional partitioning consideration is possible (and done in our implementation). Additionally, the methods are supposed to be easily extensible to function in a multi-worker setting. However, again for simplicity, their analysis is sketched without reference to the number of workers.

### `BasicTopKPlan`

**Overview of evaluation strategy**: We build a stack of arrangements, each produced by a reduction. Each reduction works on a set of top-k groups. The groups are formed by a composite key, namely a pair `(group_by_key, hash_modulus_key)`. Here, the second component of the pair is the more interesting one. A hash of the row is taken modulo a predefined factor for each level of the hierarchy. These factors are chosen such that a merge structure is created. [Here](https://github.com/MaterializeInc/materialize/blob/1636215a8e95d5519052c1b8cd32feefe35706fd/src/compute/src/render/top_k.rs#L131) is the code where the factors are declared. We build the key in the following fragments: 1) [Group by + row hash first](https://github.com/MaterializeInc/materialize/blob/1636215a8e95d5519052c1b8cd32feefe35706fd/src/compute/src/render/top_k.rs#L119); 2) Then, we [map the second component in the pair modulo the factor of that top-k stage](https://github.com/MaterializeInc/materialize/blob/1636215a8e95d5519052c1b8cd32feefe35706fd/src/compute/src/render/top_k.rs#L176).

The bottom arrangement in the reduction stack is “fat”: it has many keys for a given intended top-k group; the top arrangement will be “thin”: it has one key for the final top-k group. Each key in the arrangement holds a number of values that is at worst proportional to (not necessarily equal to) $k$. The height of the stack is logarithmic in an upper bound $N$ to the total number of rows $n$ (currently taken as the size of the 64-bit integer domain). At every level, we emit all rows in the group and retract the rows that are in the partial top-k. Again at every level, these results are negated and concatenated with the input for that level, thus resulting in only the partial top-k rows. This mechanic allows for retractions outside of the partial top-k rows to be absorbed at a given level and not propagated upwards. Additionally, it reduces memory usage at each level by obviating keeping input rows more than once per level. At the last level, the final top-k elements are produced.

**Summarized Analysis**

- Construction work:  $O(n \cdot \log k \cdot \log N)$, where $n$ is the snapshotted collection size and $N$ is the maximum size $n$ can take.
    - Argument: We have $\log N$ arrangements built by reductions. At some of these, we hit the case where we `must_shrink` (see [code](https://github.com/MaterializeInc/materialize/blob/11972046d5d186bd1e80945409c6c48be3bede01/src/compute/src/render/top_k.rs#L185-L188)). There, the $n$ rows form groups of size proportional to $k$, which are sorted. Subsequently, we just propagate partial top-k rows up doing at most the same amount of work at each level. Since there are at most $n/k$ groups at the stage where we `must_shrink`, we get $O(n \cdot \log k \cdot \log N)$.
- Update (1 row) work: $O(k \cdot \log k \cdot \log N)$
    - Argument: When an update flows in at a given level, either the update changes the partial top-k rows or not. In any case, the output at that level is recomputed by the reduction operator, resulting in a comparison sort of a number of rows proportional to $k$. If the update can be absorbed at the given level, e.g., it does not change the partial top-k rows, no further work is produced. However, in the worst case, the update may propagate to all the $O(\log N)$ upper levels, requiring a similar amount of work at each level.
- Note that we assume that the construction work for `negate`/`concat`/other is $O(n)$ and the update work is proportional to the size of the change (so $O(1)$ for 1 update). Additionally, we are sorting the input rows for the reduction, but this $O(n\cdot \log n)$ cost is also bound by the other factors articulated. Thus, these additional costs do not change the overall complexity of the operations argued above.

### `MonotonicTopKPlan`

**Overview of evaluation strategy:** The strategy is similar to the construction for the `BasicTopKPlan`; however, the monotonic strategy makes changes that affect how much state from the input collection ends up being retained. Essentially, it is observed that the input can be “thinned” once the top-k rows are determined, since all “loser” rows outside of the top-k can never again make it to the top-k set. The latter is because the input is monotonic, so there are no input retractions. The rendering thus removes all non-top-k rows from the input after the top-k set is found. This rendering strategy is not applied when an offset is given.

The behavior above is concretely implemented by a feedback operator, triggered by the usage of delay and a variable [here](https://github.com/MaterializeInc/materialize/blob/7dbd4f0aa8a454911bf92a8cebd7b1fc08491b41/src/compute/src/render/top_k.rs#L68-L75). This strategy results in an even more complex rendering than for `BasicTopKPlan`.

**Summarized Analysis:**

- Construction and update (1 row) work are of the same cost as `BasicTopKPlan`. However, a substantial amount of memory can be released for all the input rows that are never going to make it into the top-k elements after the initial top-k elements are computed.
- One key disadvantage of this strategy is that we create a memory spike that will later be “cleared out” by the feedback loop, since many retractions need to be emitted to clean the input rows that will not make it to the top-k at later times.

### `MonotonicTop1Plan`

**Overview of evaluation strategy:** We move each row to the `diff` field by wrapping it in a `Top1Monoid`, and then use a reduction to keep only the top row.

**Summarized Analysis:**

- Construction work: $O(n \log n)$
    - Argument: We need to sort the rows for the reduction.
- Update (1 row) work: $O(\log n)$
    - Argument: Similar to the above, but observing that we only need to propagate now 1 row through the reduction.
- Note that in this strategy, memory consumption is minimal, which is a great property to have.
- However, the implementation currently suffers from excessive memory allocation: https://github.com/MaterializeInc/database-issues/issues/2296. An interesting fix has been suggested in [this PR](https://github.com/MaterializeInc/materialize/pull/16782).

## Alternatives: Potential Improvements

The strategies described in the previous section have the interesting property that they are resilient to large $k$, e.g., even comparable to $n$, or skew in the formation of top-k groups. However, there may be opportunities for improvements when evaluation is restricted to small values of $k \ll n$. We are particularly interested in improvements that to monotonic top-k rendering, since the current rendering exhibits an allocation spike with a multiplicative effect due to the arrangement stack created in `BasicTopKPlan`.

In the descriptions, we gloss over the details of row multiplicities for simplicity in understanding. An implementation will need to consider these details to be correct.

### `MonotonicTopKPlan` Only

It is known from [literature](https://dl.acm.org/doi/pdf/10.1145/253262.253302) that the top-k rows in a traditional query processing setting can be computed by keeping a priority queue in $O(n \cdot \log k)$. The setting is similar to having monotonic sources, but we must deal with the additional complexities of maintenance by additions as well as formulating the strategy in a manner that is idiomatic for implementation in DD.

**Alternative 1: `TopKMonoid`**

**Overview of evaluation strategy:** We create a `TopKMonoid` difference type for `MonotonicTopKPlan` that keeps a collection of up to $k$ rows in a priority queue, namely encoded as a max-heap. With this data structure, we can combine two instances in amortized $O(k)$ since the each data structure is at most of size $k$. Now, we could have a special case for combining an instance with another that only contains a single element. In this case, either the single element should not make it into the partial top-k, being discarded, or should dislodge the maximum element in the partial top-k, which can be achieved in $O(\log k)$.

**Summarized Analysis:**

- Construction work: $O(n \cdot \log n \cdot k)$
    - Argument: for the reduction, we need to sort the input rows. Since we cannot assume a particular merge strategy for the monoids, we may unfortunately hit the amortized $O(k)$ merge cost in the worst case.
- Update (1 row) work: $O(\log n \cdot \log k)$
    - Argument: We must propagate the update through the reduction. Unlike in the construction case, here we hit the special case of combining the current top-k elements with an additional single element, which would cost $O(\log k)$.
- While this strategy would be useful for very small values of k, it is likely that the multiplication of $n$ and $k$ here can quickly become a problem.

**Alternative 2: Special-Purpose Operator**

**Overview of evaluation strategy:** We create a special-purpose operator that implements the classic strategy of keeping a priority queue, namely as a max-heap. However, we are guaranteed to keep a single priority queue per instance of the operator instead of having to merge priority queues as in the `TopKMonoid` strategy. Note that we could run an instance of the operator per Timely worker, so we need a final top-k stage that performs a reduction merging the partial top-k results produced by each operator instance.

**Summarized Analysis:**

- Construction work: $O((n + k)\cdot \log k)$.
    - Argument: The specialized operator runs the classic algorithm from the literature. Additionally, the final reduction is performed only on partial top-k results, where each such set is of size at most $k$.
- Update (1 row) work: $O(\log k)$
    - Argument: An update will change one priority queue maintained by an instance of the specialized operator. It will then bubble up the change, if necessary, through the reduction.
- Note that depending on the implementation, the strategy may still suffer from excessive memory allocation, as in the case of `MonotonicTop1Plan` above. So some care needs to be exercised when maintaining a data structure to compute the top-k vs. ownership/copying of data.

### `BasicTopKPlan` and `MonotonicTopKPlan`

**Alternative 3: Special-Purpose Operator**

**Overview of evaluation strategy:** We can create a special-purpose operator that computes partial top-k results per Timely worker, which are then combined by a final top-k stage performing a reduction. Even though at a high level this strategy is similar to Alternative 2 above, the special-purpose operator needs to be carefully constructed since we cannot rely on monotonicity. In particular, an outline of the necessary data structures could be:

- A B-tree data structure containing all of the rows seen by the operator so far. Let's call it the row B-tree.
- A B-tree data structure with the partial top-k elements. Let's call it the top-k B-tree.

For every incoming row, if we are *adding* the row, we first record it in the row B-tree data structure. Then, we check if the row would make it into the top-k by querying the top-k B-tree for its maximum. If not, the row is discarded; otherwise, we dislodge the maximum element and insert the row into the top-k B-tree.

If, on the other hand, we are *retracting* the row, we first delete the row from the row B-tree. Then, we check if the row is in the top-k elements by looking it up in the top-k B-tree. If not, then nothing else needs to be done. Otherwise, we remove the row from the top-k B-tree. Then, we query the top-k B-tree to find the new maximum element $e$. Now, we can query the row B-tree to find the next element that is greater than* $e$. This element is then inserted into the top-k B-tree.

* Note that when we consider multiplicities, this may be in fact greater than or equal!

**Summarized Analysis:**

- Construction work: $O(n \cdot \log n + (n+k)\cdot \log k)$
    - Argument: For each input row, the processing in the specialized operator requires a constant number of logarithmic cost single-row operations on the row B-tree and on the top-k B-Tree.  Then, the final reduction operates only on partial top-k results, where each such set is of size at most $k$.
- Update (1 row) work: $O(\log n + \log k)$
    - Argument: Similarly to the above, but with only a single-row update flowing through the specialized operator and the reduction.
- If we would consider building Alternative 2 for the `MonotonicTopKPlan` plan, we might wish to instead build this alternative to the `BasicTopKPlan` and not update the row B-tree when monotonicity can be asserted. However, the implementation complexity is higher overall. Additionally, with large $k$ comparable to $n$, we would be keeping 2x the input size allocated in this alternative.
- Another complexity in this alternative is that it is hard to generalize to partially ordered times, as it would be necessary to keep multiple instances of the row B-tree.

## Implemented Strategy

A variation of Alternative 2 ended up being implemented in [this PR](https://github.com/MaterializeInc/materialize/pull/16813). Similarly to Alternative 2, a special-purpose operator computes top-k results per worker as a thinning stage per timestamp. However, a data structure different from a binary heap is employed to reduce memory allocations. This leads to a slight asymptotic loss, aligning with $O(n \cdot \log n)$ for construction.

In order to bound results from an inter-timestamp perspective, the feedback loop is maintained, but now operating only on pre-thinned input. We can then assert that the input to the final top-k reduction is bounded by `num_workers * k * constant_number_of_timestamps`. As such, it is safe to render a final top-k reduction with a single stage, instead of a full stack of arrangements as for the `BasicTopKPlan`. By reusing the existing rendering for the final top-k stage, update (1 row) complexity aligns with  and $O(k \cdot \log k)$, being thus favorable for small $k$.
