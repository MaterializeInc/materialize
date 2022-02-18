# Materialize Formalism

Materialize is a system that maintains views over changing data.

At Materialize's heart are *time-varying collections* (TVCs).
These are multisets of typed *data* whose contents may vary arbitrarily as function of some ordered *time*.
Materialize records, transforms, and presents the contents of time varying collections.

Materialize's primary function is to provide the contents of time-varying collection at specific times with absolute certainty.

# Time-varying collections

Materialize represents TVCs as a set of "updates" of the form `(data, time, diff)`.
The difference `diff` represents the change in the multiplicity of `data` as we go from `time-1` to `time`.
To go from a sequence of collections to this representation one subtracts the old from new frequencies for each `data`.
To go from this representation to a sequence of collections, one updates the multiplicities of each `data` by `diff` for each `time`.

Materialize most often only has partial information about TVCs.
A TVC's future is usually not yet completely known, and its deep past also may not be faithfully recorded.
To represent what Materialize knows with certainty, Materialize maintains two "frontiers" for each TVC.
A frontier is a lower bound on times, and is used to describe the set of times greater or equal to the lower bound.
The two frontiers Materialize maintains for each TVC are:

*   The `since` frontier, which communicates that for times `time` greater or equal to `since`, the accumulated collection at `time` is precise.

    Informally, updates at times not greater than the `since` frontier may have been consolidated with other updates.
    They are not discarded, but their times may have been advanced to the `since` frontier to allow cancelation and consolidation of updates to occur.
    This is analogous to "version vacuuming" in multiversioned databases.
    As the system operates, the `since` frontier may advance to allow compaction of data.
    This happens subject to constraints, and only when correctness permits.

    We think of `since` as the "read frontier": times greater or equal to it may still be correctly read from the TVC.

*   The `upper` frontier, which communicates that for times `time` not greater or equal to `upper`, the updates at `time` are complete.

    The upper frontier provides the information about how a TVC may continue to evolve.
    All future updates will necessarily have their `time` greater or equal to the `upper` frontier.
    As the system operates, we expect the `upper` frontiers to advance indicating that the contents of the collection are known for more times.
    This happens only when we become certain that updates for a time are complete.

    We think of `upper` as the "write frontier": times greater or equal to it may still be written to the TVC.

These two frontiers mean that for any `time` greater or equal to `since`, but not greater or equal to `upper`, we can exactly reconstruct the collection at `time`.
If `time` is not greater or equal to `since`, we may have permanently lost historical distinctions that allow us to know the contents at `time`.
If `time` is greater or equal to `upper`, we may still receive changes to the collection that occur at `time`.
The two frontiers never move backward, and often move forward as the system operates.

At each moment, Materialize's representation of a TVC is as a set of update triples `(data, time, diff)` and the two frontiers `since` and `upper`.
We call this pair of set of updates and `(since, upper)` frontiers a *partial TVC* or pTVC.

## Evolution

Materialize maintains a (partial) map from `GlobalId`s to pTVCs.
A `GlobalId` is never re-used to mean a different TVC.

The life cycle of each `GlobalId` follows several steps:

0. The `GlobalId` is initially not yet bound.

    In this state, the `GlobalId` cannot be used, as we do not yet know what its initial `since` will be.

1. The `GlobalId` is bound to some definition, with an initially equal `since` and `upper`.

    This means that the collection is initially not useful, as it cannot be queried at any time greater or equal to `since` but not greater or equal to `upper`.
    However, the initial value of `since` indicates that one should expect the data to eventually become available for each time greater or equal to `since`.
    The partial TVC can now be used in queries, though results may not be immediately available.

2. Repeatedly, either of the these two steps occur:

    a. The `upper` frontier advances, revealing more of the future of the pTVC.

    b. The `since` frontier advances, concealing some of the past of the pTVC.

3. Eventually, the `since` frontier advances to the empty collection, rendering the TVC complete and closed.

The `GlobalId` is never reused.

# Storage and Compute

Materialize produces and maintains time-varying collections in two distinct ways:

*   The **Storage** layer records explicit TVCs in response to direct instruction (e.g. user DML), integrations with external sources of data, and feedback from Materialize itself.
    The representation in the Storage layer is as explicit update triples and maintained `since` and `upper` frontiers.
    The primary property of the Storage layer is that, even across failures, it is able to reproduce all updates for times that lie between `since` and `upper` frontiers that it has advertised.

*   The  **Compute** layer uses a view definition to transform input TVCs into the exactly corresponding output TVCs.
    The output TVC varies *exactly* with the input TVCs:
    For each `time`, the output collection at `time` equals the view applied to the input collections at `time`.

The Storage and Compute layers both track `since` and `upper` frontiers for the collections they manage.
For both layers, the `since` frontier is controlled by their users and it does not advance unless it is allowed.
The `upper` frontier in Storage is advanced when it has sealed a frontier and made the data up to that point durable.
The `upper` frontier in Compute is advanced when its inputs have advanced and its computation has pushed updates fully through the view definitions.

## Capabilities

The `since` and `upper` frontiers are allowed to advance through the use of "capabilities".

* `ReadCapability(id, frontier)` prevents the `since` frontier associated with `id` from advancing beyond `frontier`.
* `WriteCapability(id, frontier)` prevents the `upper` frontier associated with `id` from advancing beyond `frontier`.

A capability presents the `id` and `frontier` values, but is identified by a system-assigned unique identifier.
Any user presenting this identifier has the associated capability to read from or write to the associated pTVC.
Actions which read or write will validate the capability (e.g. in case a lease has expired) and may return errors if the capability is not valid as stated.

Capabilities are durable, and their state is tracked by the system who associates a unique identifier with each.
Capabilities can be cloned and transfered to others, providing the same rights (and responsibilities).
Capabilities can be downgraded to frontiers that are greater or equal to the held frontier, which may then allow the `since` or `upper` frontier to advance.
Capabilities can be dropped, which releases the constraint on the frontier, and the whichever ability (read or write) the capability provided.
These actions are mediated by the system, and take effect only when the system confirms the action.
Users can defer and batch these actions, modulo lease timeouts, imposing only on the freshness and efficiency of the system.

Capabilities may expire, as a lease, which may result in errors from attempts to exercise the capability.

It is very common for users to immediately drop capabilities if they do not require the ability.
The write capability for most TVCs is dropped immediately (by the system) unless it is meant to be written to by users (e.g. a `TABLE`).
The read capability may be dropped immediately if the user does not presently need to read the data.
Capabilities may be recovered by the best-effort command `AcquireCapabilities(id)` to be discussed.

## Storage

The Storage layer presents a narrow API about which it makes guarantees.
It likely has a more verbose diagnostic API that describes its state, which should be a view over the results of these commands.

*   `Create(id, description) -> (ReadCapability, WriteCapability)`: binds to `id` the TVC described by `description`.

    The command returns capabilities naming `id`, with frontiers set to initial values chosen by the Storage layer.

    The collection associated with `id` is based on `description`, but it is up to the Storage layer to determine what the source's contents will be.
    A standarad example is a Kafka topic, whose contents are "added" to the underlying collection as they are observed.
    The Storage layer only needs to ensure that it can reliably produce the updates between `since` and `upper`, and otherwise the nature of the content is up for discussion.
    It is an error to re-use a previously used `id`.

*   `SubscribeAt(ReadCapability(id, frontier))`: returns a snapshot of `id` at `frontier`, followed by an ongoing stream of subsequent updates.

    This command returns the contents of `id` as of `frontier` once they are known, and updates thereafter once they are known.
    The snapshot and stream contain in-line statements of the subscription's `upper` frontier: those times for which updates may still arrive.
    This call may block, or not be promptly responded to, if `frontier` is greater or equal to the current `upper` of `id`.
    The subscription can be canceled by either endpoint, and the recipient should only downgrade their read capability when they are certain they have all data for an interval.

    A subscription can be constructed with additional arguments that change how the data is returned to the user.
    For example, the user may ask to not receive the initial snapshot, rather that receive and then discard it.
    For example, the user may provide filtering and projection that can be applied before the data are transmitted.

*   `UpdateAndDowngrade(WriteCapability(id, frontier), updates, new_frontier)`: applies `updates` to `id` and downgrades `frontier` to `new_frontier`.

    All times in `updates` must be greater or equal to `frontier` and not greater or equal to `new_frontier`.

    This is the analog of the `INSERT` statement, though `updates` are signed and general enough to support `DELETE` and `UPDATE` at the same time.

*   `AcquireCapabilities(id) -> (ReadCapability, WriteCapability)`: provides capabilities for `id` at its current `since` and `upper` frontiers.

    This method is a best-effort attempt to regain control of the frontiers of a collection.
    Its most common uses are to recover capabilities that have expired (leases) or to attempt to read a TVC that one did not create (or otherwise receive capabilities for).
    If the frontiers have been fully released by all other parties, this call may result in empty frontiers (useless).

The Storage layer provides its output through `SubscribeAt` and `AcquireCapabilities`, which reveal the contents and frontiers of the TVC.
All commands are durable upon the return of the invocation.

## Compute

The Compute layer presents one additional API command.
The layer also intercepts commands for identifiers it introduces.

*   `MaintainView([ReadCapabilities], view, [WriteCapabilities]) -> [ReadCapabilities]`: installs a view maintenance computation described by `view`.

    Each view is described by a set of input read capabilities, identifying collections and a common frontier.
    The `view` itself describes a computation that produces a number of outputs collections.
    Optional write capabilities allow the maintained view to be written back to Storage.
    The method returns read capabilities for each of its outputs.

    The Compute layer retains the read capabilities it has been offered, and only downgrades them as it is certain they are not needed.
    For example, the Compute layer will not downgrade its input read capabalities past the `since` of its outputs.

The outputs of a dataflow may or may not make their way to durable TVCs in the Storage layer.
All commands are durable upon return of the invocation.
