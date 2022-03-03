# Materialize Formalism

Materialize is a system that maintains views over changing data.

At Materialize's heart are *time-varying collections* (TVCs).
These are multisets of typed *data* whose contents may vary arbitrarily as function of some ordered *time*.
Materialize records, transforms, and presents the contents of time varying collections.

Materialize's primary function is to provide the contents of time-varying collection at specific times with absolute certainty.

This document details the **intended behavior** of the system.
It does not intend to describe the **current behavior** of the system.
Various liberties are taken in the names of commands and types, in the interest of simplicity.
Not all commands here may be found in the implementation, nor all implementation commands found here.

# Time-varying collections

Materialize represents TVCs as a set of "updates" of the form `(data, time, diff)`.
The difference `diff` represents the change in the multiplicity of `data` as we go from `time-1` to `time` (for times that form a sequence; for partially ordered times consult the Differential Dataflow source material for more details).
To go from a sequence of collections to this representation one subtracts the old from new frequencies for each `data`.
To go from this representation to a sequence of collections, one updates the multiplicities of each `data` by `diff` for each `time`.

Materialize most often only has partial information about TVCs.
A TVC's future is usually not yet completely known, and its deep past also may not be faithfully recorded.
To represent what Materialize knows with certainty, Materialize maintains two "frontiers" for each TVC.
A frontier is a lower bound on times, and is used to describe the set of times greater or equal to the lower bound.
The two frontiers Materialize maintains for each TVC are:

*   The `upper` frontier, which communicates that for times `time` not greater or equal to `upper`, the updates at `time` are complete.

    The upper frontier provides the information about how a TVC may continue to evolve.
    All future updates will necessarily have their `time` greater or equal to the `upper` frontier.
    As the system operates, the `upper` frontiers advance when the contents of the collection are known with certainty for more times.

    We think of `upper` as the "write frontier": times greater or equal to it may still be written to the TVC.

*   The `since` frontier, which communicates that for times `time` greater or equal to `since`, the accumulated collection at `time` will be correct.

    Informally, updates at times not greater than the `since` frontier may have been consolidated with updates at other times.
    The updates are not discarded, but their times may have been advanced to allow cancelation and consolidation of updates to occur.
    This is analogous to "version vacuuming" in multiversioned databases.
    As the system operates, the `since` frontier may advance to allow compaction of updates.
    This happens subject to constraints, and only when correctness permits.

    We think of `since` as the "read frontier": times greater or equal to it may yet be correctly read from the TVC.

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

    In this state, the `GlobalId` cannot be used in queries, as we do not yet know what its initial `since` will be.

1. The `GlobalId` is bound to some definition, with an initially equal `since` and `upper`.

    The partial TVC can now be used in queries, those reading from `since` onward, though results may not be immediately available.
    The collection is initially not useful, as it will not be able to immediately respond about any time greater or equal to `since` but not greater or equal to `upper`.
    However, the initial value of `since` indicates that one should expect the data to eventually become available for each time greater or equal to `since`.

2. Repeatedly, either of the these two steps occur:

    a. The `upper` frontier advances, revealing more of the future of the pTVC.

    b. The `since` frontier advances, concealing some of the past of the pTVC.

3. Eventually, the `upper` frontier advances to the empty set of times, rendering the TVC unwriteable.

3. Eventually, the `since` frontier advances to the empty set of times, rendering the TVC unreadable.

The `GlobalId` is never reused.

# Storage and Compute

Materialize produces and maintains bindings of `GlobalId`s to time-varying collections in two distinct ways:

*   The **Storage** layer records explicit TVCs in response to direct instruction (e.g. user DML), integrations with external sources of data, and feedback from Materialize itself.
    The representation in the Storage layer is as explicit update triples and maintained `since` and `upper` frontiers.
    The primary property of the Storage layer is that, even across failures, it is able to reproduce all updates for times that lie between `since` and `upper` frontiers that it has advertised.

*   The  **Compute** layer uses a view definition to transform input TVCs into the exactly corresponding output TVCs.
    The output TVC varies *exactly* with the input TVCs:
    For each `time`, the output collection at `time` equals the view applied to the input collections at `time`.

The Storage and Compute layers both manage the `since` and `upper` frontiers for the `GlobalId`s they manage.
For both layers, the `since` frontier is controlled by their users and it does not advance unless it is allowed.
The `upper` frontier in Storage is advanced when it has sealed a frontier and made the data up to that point durable.
The `upper` frontier in Compute is advanced when its inputs have advanced and its computation has pushed updates fully through the view definitions.

## Capabilities

The `since` and `upper` frontiers are allowed to advance through the use of "capabilities".
Each capability names a `GlobalId` and a frontier.

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
    The subscription will produce exactly correct results: the snapshot is the TVCs contents at `frontier`, and all subsequent updates occur at exactly their indicated time.
    This call may block, or not be promptly responded to, if `frontier` is greater or equal to the current `upper` of `id`.
    The subscription can be canceled by either endpoint, and the recipient should only downgrade their read capability when they are certain they have all data through the frontier they would downgrade to.

    A subscription can be constructed with additional arguments that change how the data is returned to the user.
    For example, the user may ask to not receive the initial snapshot, rather that receive and then discard it.
    For example, the user may provide filtering and projection that can be applied before the data are transmitted.

*   `UpdateAndDowngrade(WriteCapability(id, frontier), updates, new_frontier)`: applies `updates` to `id` and downgrades `frontier` to `new_frontier`.

    All times in `updates` must be greater or equal to `frontier` and not greater or equal to `new_frontier`.

    It is probably an error to call this with `new_frontier` equal to `frontier`, as it would mean `updates` must be empty.
    This is the analog of the `INSERT` statement, though `updates` are signed and general enough to support `DELETE` and `UPDATE` at the same time.

*   `AcquireCapabilities(id) -> (ReadCapability, WriteCapability)`: provides capabilities for `id` at its current `since` and `upper` frontiers.

    This method is a best-effort attempt to regain control of the frontiers of a collection.
    Its most common uses are to recover capabilities that have expired (leases) or to attempt to read a TVC that one did not create (or otherwise receive capabilities for).
    If the frontiers have been fully released by all other parties, this call may result in capabilities with empty frontiers (which are useless).

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

### Constraints

A view introduces *constraints* on the capabilities and frontiers of its input and output identifiers.

*   The view holds a write capability for each output that does not advance beyond the input write capabilities.

    This prevents the output from announcing it is complete through times for which the input may still change.
    The output write capabilities are advanced through timely dataflow, which tracks outstanding work and can confirm completeness.

*   The view holds a read capability for each input that does not advance beyond the output read capabilities.

    This prevents the compaction of inputs updates into a state that prevents recovery of the outputs.
    Outputs may need to be recovered in the case of failure, but also in other less dramatic query migration scenarios.

These constraints are effected using the capability abstractions.
The Compute layer acquires and maintains read capabilities for collections managed by it, and by the Storage layer.

# Adapter

The adapter layer translates SQL statements into commands for the Storage and Compute layers.

The most significant differences between SQL statements and commands to the Storage and Compute layers are:
1.  The absence of explicit timestamps in SQL statements.

    A `SELECT` statement does not indicate *when* it should be run, or against which version of its input data.
    The Adapter layer introduces timestamps to these commands, in a way that provides the appearance of sequential execution.

2.  The use of user-defined and reused names in SQL statements rather than `GlobalId` identifiers.

    SQL statements reference user-assigned names that may be rebound over time.
    The meaning, and even the validity, of a query depends on the current association of user-defined name to `GlobalId`.
    The Adapter layer maintains a map from user-assigned names to `GlobalId` identifiers, and introduces new ones as appropriate.

Generally, SQL is "more ambiguous" than the Storage and Compute layers, and the Adapter layer must resolve that ambiguity.

The SQL commands are a mix of DDL (data definition language) and DML (data manipulation language).
In Materialize today, the DML commands are timestamped, and the DDL commands are largely not (although their creation of `GlobalId` identifiers is essentially sequential).
That DDL commands are not timestamped in the same sense as a pTVC is a known potential source of apparent consistency issues.
While it may be beneficial to think of Adapter's state as a pTVC, changes to its state are not meant to cascade through established views definitions.

Commands acknowledged by the Adapter layer are durably recorded in a total order.
Any user can rely on all future behavior of the system reflecting any acknowledged command.

## Timelines

The locus of consistency in Materialize is called a timeline.
A timeline is a durable stateful object that assigns timestamps to a sequence of DML statements, such that
1. The timestamps never decrease.
2. The timestamps of INSERT/UPDATE/DELETE statements are strictly greater than those of preceding SELECT statements.
3. The timestamps of SELECT statements are greater than or equal to the read capabilities of their input collections.
4. The timestamps of INSERT/UPDATE/DELETE statements are greater than or equal to the write capabilities of their target collection.

All commands are serialized through the timeline.
A timeline is not a complicated concurrent object.

The timeline timestamps do not need to strictly increase, and any events that have the same timestamp are *concurrent*.
Concurrent events (at the same timestamp) first modify collections and then read collections.
All writes at a timestamp are visible to all reads at the same timestamp.

SELECT statements observe all data mutations up through and including their timestamp.
For this reason, we strictly advance the timestamp for each write that occurs after a read, to ensure that the write is not visible to the read.
ADAPTER uses `UpdateAndDowngrade` in response to the first read after a write, to ensure that prior writes are readable and to strictly advance the write frontier.

Some DML operations, like UPDATE and DELETE, require a read-write transaction which prevents other writes from intervening.
In these and other non-trivial cases, we rely on the total order of timestamps to provide the apparent total order of system execution.

---

Several commands support an optional `AS OF <time>` clause, which instructs the Adapter to use a specific query `time`, if valid.
The presence of this clause opts a command out of the timeline reasoning: it is neither constrainted by nor does it constrain timestamp selection for other commands.
The command will observe all writes up through `time` (if it reads) and be visible by all reads from `time` onward (if it writes).

---

## Sources

Materialize supports a `CREATE SOURCE` command that binds to a user-specified name a `GlobalId` produced by the Storage layer in response to a `Create` command.
A created source has an initial read capability and write capability, which may not be at the beginning of time.
Two sources created with the same arguments are not guaranteed to have the same contents at the same time (said differently: Materialize uses *nominal* rather than *structural* equivalence for sources).

Sources remain active until a `DROP SOURCE` command is received, at which point the Adapter layer drops its read capability.

One common specialization of source is the "table".
The `CREATE TABLE` command introduces a new source that is not automatically populated by an external source, and is instead populated by `UpdateAndDowngrade` commands.
The Adapter layer may use a write-ahead log to durably coalesce multiple writes at the same timestamp, as the `UpdateAndDowngrade` command does not otherwise allow this.
The `DROP TABLE` command drops both the read and write capabilities for the source.

## Indexes

Materialize supports a `CREATE INDEX` command that results in a dataflow that computes and maintains the contents of the index's subject, in indexed representation.
The `CREATE INDEX` command can be applied to any collection whose definition avoids certain non-deterministic functions (e.g. `now()`, `rand()`, environment variables).

Materialize's main feature is that for any indexable view, one receives identical results whether re-evaluating the view from scratch or reading it from a created index.

The `CREATE INDEX` command results in a `MaintainView` command for the Compute layer.
The inputs to that command are `GlobalId` identifiers naming Storage or Compute collections.
The command is issued with read capabilities all advanced to a common frontier (often: the maximum frontier of read capabilities in the input).
The command returns a read capability for the indexed collection, which the Adapter layer maintains and perhaps regularly downgrades, as is its wont.
The dataflow remains active until a corresponding `DROP INDEX` command is received by the Adapter layer, which discards its read capability for the indexed collection.

## Transactions

The Adapter layer provides interactive transactions through the `BEGIN`, `COMMIT`, and `ROLLBACK` commands.
There are various restrictions on these transactions, primarily because the Storage and Compute layers require you to durably write before you can read from them.
This makes read-after-write transactions challenging to perform efficiently, and they are not currently supported.
Write-after-read transactions are implemented by performing all reads at the current timestamp and then writing at a strictly later timestamp.
Other read-only transactions may co-occur with a write-after-read transaction (logically preceding the transaction, as its writes are only visible at its write timestamp).
Other write-only transactions may precede or follow a write-after-read transaction, but may not occur *between* the read and write times of that transaction.
Multiple write-after-read transactions may not co-occur (their [read, write) half-open timestamp intervals must be disjoint).

It is possible that multiple write-after-read transactions on disjoint collections could be made concurrent, but the technical requirements are open at the moment.

Read-after-write, and general transactions are technically possible using advanced timestamps that allow for Adapter-private further resolution.
All MZ collections support compensating update actions, and one can tentatively deploy updates and eventually potentially retract them, as long as others are unable to observe violations of atomicity.
Further detail available upon request.

It is critical that Adapter not deploy `UpdateAndDowngrade` commands to Storage and Compute until a transaction has committed.
These lower layers should provide similar "transactional" interfaces that validate tentative commands and ensure they can be committed without errors.

## Compaction

As Materialize runs, the Adapter may see fit to "allow compaction" of collections Materialize maintains.
It does so by downgrading its held read capabilities for the collection (identifier by `GlobalId`).
Downgraded capabilities restrict the ability of Adapter to form commands to Storage and Compute, and may force the timeline timestamps forward.
Generally, the Adapter should not downgrade its read capabilities past the timeline timestamp, thereby avoiding this constraint.
