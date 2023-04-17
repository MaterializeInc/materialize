- Feature name: Faster Dataflow Shutdown
- Associated:
    * <https://github.com/MaterializeInc/materialize/issues/2392>
    * <https://github.com/MaterializeInc/materialize/issues/7577>
    * <https://github.com/MaterializeInc/materialize/issues/16800>


# Summary
[summary]: #summary

COMPUTE lets dataflows shut down gracefully once they are not needed anymore, by making the sources close their outputs and then waiting for the empty frontier to cascade through the whole dataflow.
This strategy has the disadvantage of potentially taking a very long, or even infinite, time.
As a result, dataflows may continue to consume significant system resources even after the user has instructed us to drop them.
This design proposes mechanism to mitigate some of these issues.
All proposed mechanisms are based on the existing token-based approach to shutting down dataflows in a cooperative manner.
They are meant as best-effort alternatives to active dataflow cancellation using Timely's `drop_dataflow` API, which we currently consider not ready for production use.


# Motivation
[motivation]: #motivation

COMPUTE uses Timely Dataflow to run dataflows.
Timely provides an API, [`Worker::drop_dataflow`], to drop any installed dataflow.
However, this API is mostly untested and generally considered not production ready by the maintainers of Timely Dataflow ([timely-dataflow/#519]).
Consequently, it is not used in Materialize.

Instead, our dataflow shutdown strategy is based on the observation that a dataflow operator will become descheduled and cleaned up once Timely determines that it can't produce any more output, i.e., when it has advanced to the empty frontier.
A well-behaved operator advances to the empty frontier eventually once all its inputs have advanced to the empty frontier.
Therefore, to shut down a dataflow it is usually sufficient to ensure that its sources advance to the empty frontier.
The remaining operators then finish processing all updates that where previously emitted and subsequently advance their outputs to the empty frontier as well.

To shut down a dataflow, COMPUTE signals the source operators, causing them to stop emitting updates for new times and to instead advance to the empty frontier.
This transitively causes a shutdown of all operators in the dataflow.

This approach works well for simple dataflows, but it tends to fall short when dataflows become more complex or contain more data.
The following is a list of problems we have observed so far:

- **Divergent dataflows run forever** [#16800]

  Since the introduction of [`WITH MUTUALLY RECURSIVE` (WMR)][WMR], it is possible to construct divergent dataflows that stop making progress in their outputs.
  For example:

  ```sql
  WITH MUTUALLY RECURSIVE
      flip(x int) AS (VALUES(1) EXCEPT ALL SELECT * FROM flip)
  SELECT * FROM flip;
  ```

  Such dataflows contain loops that are not interrupted by the dataflow inputs shutting down, so the strategy described above does not work.
  The divergent loop part of the dataflow, and all operators downstream of it, continue running forever.
  The only way to stop such a dataflow is currently to drop the affected replicas.

- **Persist sources always emit full batches**

  Persist sources don't shut down immediately when signaled to do so.
  Instead, they always finishes emitting the remaining updates available at the current time.
  This behavior is intentional and was introduced to work around COMPUTE's inability to handle partially-emitted times ([#16860]).
  Nevertheless, it inhibits timely dataflow shutdown when the current batch is large.

  Consider, for example, a `SELECT` query that reads from a large storage collection.
  Shortly after issuing the query the user notices that the result will likely not fit into memory, so they cancel it.
  However, the persist source is implemented to still emit the entire snapshot of the source collection and pass that data downstream.
  So even though the user was quick to cancel the query it might still eventually OOM the replica it is running on.

  Even if the replica doesn't run out of memory, the current behavior of persist source means that more data is introduced into the dataflow before it can shut down.
  This slows down the dataflow shutdown, as downstream operators require time to process the new updates in addition to the ones already in the dataflow.

- **Join operators can amplify data** [#7577]

  Depending on the amount of input data and the join condition, joins can vastly multiply their input data.
  Consequently, they can take a long time to finish emitting all their outputs even once their inputs have advanced to the empty frontier.
  Similar issues arise as described for persist sources above: Dataflow shutdown can be delayed by a significant amount of time, during which the join operator adds new data to be processed by its downstream operators, and a cancelled dataflow can still OOM the replica.

  The data-multiplication aspect is particularly painful in dataflows that contain several cross joins in sequence.
  While users will probably never *want* to install such dataflows, they might do so by accident and expect us to be able to cancel their query without causing further damage.

Failing to shut down a dataflow, or failing to do so in a timely manner, usually has a negative impact on usability:

- Dropped dataflows keep running in the background and compete with useful dataflows for system resources. This can cause noticeably reduced query responsiveness.
- Dropped dataflows keep ingesting and multiplying data, which might OOM their replica.
- Dataflows are visible in introspection sources long after they have been dropped, confusing diagnostic queries.

For these reasons, we are interested in completing dataflow shutdowns as quickly as possible.

[`Worker::drop_dataflow`]: https://dev.materialize.com/api/rust/timely/worker/struct.Worker.html#method.drop_dataflow
[timely-dataflow/#519]: https://github.com/TimelyDataflow/timely-dataflow/pull/519
[WMR]: https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20221204_with_mutually_recursive.md
[#7577]: https://github.com/MaterializeInc/materialize/issues/7577
[#16800]: https://github.com/MaterializeInc/materialize/issues/16800
[#16860]: https://github.com/MaterializeInc/materialize/issues/16860


# Explanation
[explanation]: #explanation

In COMPUTE, every dataflow source holds a reference to a token.
The COMPUTE worker also holds references to these tokens, associated with the dataflow exports fed by the respective sources.
When a worker decides that a dataflow export (e.g., and index or an MV) is no longer needed, it drops the tokens associated with it.
Dataflow sources can observe the dropping of their tokens.
Once they observe that there is no other holder of their token left, they know to shut down.

In this design we propose three measures to tackle the problems outlined in [motivation], all based on the existing token-based approach:

1. [Fuse operators](#fuse-operators)
1. [Faster persist source shutdown](#faster-persist-source-shutdown)
1. [Tokenizing join operators](#tokenizing-join-operators)

Each of these measures introduces the possibility that downstream operators receive incomplete update sets for a given time slice.
We discuss how to deal with issues introduced by this as well.

## Fuse Operators

We propose the introduction of a "fuse" operator.
The purpose of this operator is to interrupt the flow of updates when the dataflow is dropped.
This is useful to ensure downstream operators are not fed more updates when they should be shutting down.
It is also useful to construct an artificial fixpoint in dataflow loops.

In the scope of this document, the main use case for the fuse operator is forcing divergent WMR dataflows to shut down.
WMR dataflows circulate updates until they reach a fixpoint, and a WMR dataflow becomes divergent when no fixpoint can ever be reached.
By introducing a fuse operator in the loop, the circulation of updates is stopped when the dataflow is dropped, allowing the operators in the loop to advance to the empty frontier and shut down.

We anticipate that the fuse operator will also come in handy to resolve other shutdown bottlenecks.
For example, it can be inserted before expensive operators to stop their incoming update flow as early as possible.
It is also likely to be useful to support [tokenizing join operators](#tokenizing-join-operators).

The fuse operator is instantiated with a token reference that it uses to observe dataflow shutdowns.
As long as there are still other holders of the token, it simply forwards all updates received on its inputs to its outputs.
Once the token has been dropped by everyone else, it discards all input updates.

In addition to discarding updates when the token is dropped, the fuse operator can also immediately advance its output to the empty frontier.
This is not a requirement for solving divergent WMR cancellation, as dropping data updates is sufficient for reaching a fixpoint.
Rather, immediately advancing to the empty frontier is a measure to speed up shutdown of the operators downstream of the fuse operator.
It allows them to observe the empty import frontier as soon as the token is dropped, potentially long before the input to the fuse operator has produced all of its remaining outputs and advanced to the empty frontier.
We propose implementing this performance optimization in the fuse operator, even though it is not strictly required, because it has low implementation complexity and little associated risk.

[#18718] provides the described fuse operator implementation, and applies it to the cancellation of WMR dataflows.

[#18718]: https://github.com/MaterializeInc/materialize/pull/18718

## Faster Persist Source Shutdown

We propose restoring the previous behavior of the persist source of immediately shutting down when it observes that the shutdown token was dropped, rather than ignoring the shutdown until the entire current batch is emitted.
The benefit of this change is that cancellation of queries that read large snapshots from persist becomes much faster, by reducing the time until persist sources shut down, as well as the amount of data introduced into the dataflow.
Reducing the amount of introduced data can also prevent OOMs that might otherwise occur some time after the user has already cancelled a query.

The original reason for having persist sources always emit whole batches was that certain COMPUTE operators were not able to deal with only seeing an incomplete set of updates for a time.
They would see invalid retractions and occasionally react by panicking.
As part of [#17178], COMPUTE operators have been made much more robust to unexpected retractions.
They now consistently report errors gracefully instead of crashing the process.
Because of this the workaround in the persist source is not necessary anymore and we are free to revert it to the previous more efficient behavior.

[#17178]: https://github.com/MaterializeInc/materialize/issues/17178

## Tokenizing Join Operators

We propose that join operators (both linear and delta joins) receive references to shutdown tokens and use these to stop update processing when the dataflow is getting shut down.
The benefits of this change are that join operators shut down more quickly and that they are prevented from introducing more data into the dataflow.

Exactly how the join implementations should perform the token checking is still an open question.
Initial testing in the scope of [#7577] suggests that the main bottleneck in joins does not come from the work of producing matching updates, but from applying the join closure to the result of the matching.
This implies that the join closure should perform a token check before processing each join result.

Concerns have been raised that the overhead of performing a check for every update might slow down joins prohibitively.
We have not been able to corroborate these concerns with tests so far.
At least for `SELECT` queries computing simple cross joins adding a token check to the join closure does not appear to increase the query time noticeably.
It is likely that the compiler is able to lift the token check out of the loop that calls the join closure in DD, so the check is only performed once per batch rather than once per update.

There might be other places in the join implementations where adding a token check improves shutdown performance.
For example, adding a fuse operator after the join output would allow downstream operators to observe an empty input frontier and begin shutting down before the join operator has finished iterating over all matches.
In the interest of keeping code complexity in check, we suggest to not speculatively add more token checks to the join implementations, unless we have evidence (e.g., example queries) that they significantly improve dataflow shutdown performance. The same applies to adding token checks to other operators than join.

[#7577]: https://github.com/MaterializeInc/materialize/issues/7577

## Handling Incomplete Time Slices

Each of the measures proposed above relies on suppressing updates that would usually be emitted to downstream operators.
As a result, if an operator observes a shutdown while processing a time slice, its downstream operators might only receive an incomplete set of updates for this time slice.
Operators must thus be able to deal with seeing invalid data during a dataflow shutdown. In particular:

1. Operators must gracefully handle invalid retractions (i.e., updates with negative multiplicity that don't have associated positive updates).
2. Dataflow sinks must not emit invalid data to external systems.

Since the resolution of [#17178], operators observing invalid retractions don't panic anymore.
But they still log errors that appear in Sentry and alert engineering about potential bugs.
To avoid unnecessary noise, we need to ensure that no errors are logged when invalid retractions are observed due to dataflow shutdown.
For this, we propose giving all operators that can log errors due to invalid retractions handles to shutdown tokens.
These operators can then check if the dataflow is still alive before logging any errors.

Persist sinks already hold onto tokens that inform them about dataflow shutdowns and that ensure that no further data is emitted once the COMPUTE worker drops a sink token.
Indexes and subscribes are technically also dataflow sinks, albeit ones that don't directly write to external systems.
Instead, their output is queried and forwarded by the COMPUTE worker.
The worker is implemented in such a way that it stops reading from a dataflow's output before dropping it, so there is no risk of it reading invalid data from indexes or subscribes during dataflow shutdown.


# Rollout
[rollout]: #rollout

## Testing and observability
[testing-and-observability]: #testing-and-observability

We rely on our existing test suite to ensure that the above measures for faster dataflow shutdown don't impact correctness (e.g., by making operators shut down too early).

For testing that the proposed measures have an effect we will write testdrive tests that drop dataflows and observe their shutdown process with help of the introspection sources.
This will be very simple for testing cancellation of divergent WMR dataflows (which shut down either quickly or never) but harder for testing the cancellation of persist sources and cross joins (which shut down either quickly or slowly).
Since the later tests rely on timing, care must be taken to ensure they don't produce false positives or false negatives.
For divergent WMR dataflows, we will make sure to include complex mutually recursive structures, to ensure that dropping those works as expected.

For [tokenizing join operators](#tokenizing-join-operators) in particular, potential performance degradation is a concern.
We can build some confidence here by performing SQL-based benchmarks to accompany the PR that introduces this feature.

Finally, we will perform a load test that creates and drops a large number of dataflows in a staging environment over a period of several hours.
We will use this test to verify that there are no unexpected race conditions or memory leaks introduced by the mechanisms described in this design.

## Lifecycle
[lifecycle]: #lifecycle

The changes proposed in this design do not seem particularly risky.
There is no need for feature flags or extended testing on staging.


# Drawbacks
[drawbacks]: #drawbacks

The proposed changes increase the complexity of the rendering code by requiring that dataflow shutdown tokens are passed to various additional operators.

Furthermore, when compared to `drop_dataflow`, the proposed measures are less effective.
They make dataflows shut down faster but cannot guarantee immediate shutdown.


# Conclusion and alternatives
[conclusion-and-alternatives]: #conclusion-and-alternatives

The proposed design is effective in solving commonly observed issues that lead to slow dataflow shutdown and corresponding usability issues.
It is also relatively easy to implement, as it extends the token mechanism that already exists in dataflow rendering.

As an alternative to the changes proposed here, we could instead rely on Timely's `drop_dataflow`.
Doing so would simplify the implementation in Materialize, as dataflow rendering would not have to care about shutdown concerns.
`drop_dataflow` would also guarantee that dataflow operators are always shut down immediately, rather than after some undetermined amount of time.
However, the maintainers of Timely Dataflow are not confident in the correctness of `drop_dataflow` at this time.
As such, we consider relying on it for such a fundamental part of Materialize as too risky.

An alternative variant of the proposed join tokenization could be to implement the token checks not inside the Materialize rendering code, but in the core DD operator logic.
Doing so would potentially provide greater control of the update stream, which would allow us to discard updates earlier.
We reject this alternative because making changes to DD is relatively more complicated and time-intensive.
Furthermore, DD is a public library, so it is not clear whether the maintainers would accept changes that are tailored to the Materialize use-case.

<!--
# Unresolved questions
[unresolved-questions]: #unresolved-questions

- What questions need to be resolved to finalize the design?
- What questions will need to be resolved during the implementation of the design?
- What questions does this design raise that we should address separately?
-->


# Future work
[future-work]: #future-work

We expect to find more instances of dataflow operators that are slow to shut down under certain conditions, even after this design is implemented.
Insofar as these instances can be resolved by adding more token checks, we should do so.

This design does not consider issues caused by dataflow operators refusing to yield back to the Timely runtime.
Examples of this are `FlatMap`s executing `generate_series` functions with high numbers ([#8853]), or high-cardinality joins over a small number of keys.
These instances can only be solved by adding or improving fueling for these operators, which is something we should do but leave as future work.

Finally, we should continue the work of stabilizing `drop_dataflow`.
As pointed out above, it is a simpler and more effective approach than what this design proposes, and therefore a strictly better solution provided we can gain confidence in it.

[#8853]: https://github.com/MaterializeInc/materialize/issues/8853
