# Reconciling COMPUTE

The controller and compute instances are logically related but can be hosted on different processes, and the state shared between the two needs to be reconciled.
State includes for example installed dataflows, source subscriptions and sinks, as well as the current progress of the computation.
On restart, the controller wants to dictate the instances what their state should be.
The instances want to come up with this state, either by restarting or reconciling their state.
This document lines out the requirements for reconciling the state compute and storage instances, and active replication.

Initially, compute instances have no state.
As they receive commands from the controller, they evolve their local state.
Reconciliation kicks in when the system encounters a failure, such as processes terminating, network failures or logic bugs.
On each boundary, Materializes reconciles commands to maintain a local representation of the state of the system.
In the following, we explain what this means for reconciling compute commands on compute instances, storage commands on storage instances, and response reconciliation to enable active replication within the coordinator.

## Compute command reconciliation

Commands represent instructions the COMPUTE controller sends to COMPUTE instances.
Compute command reconciliation (CR) is hosted by each compute instance.
* The COMPUTE controller may restart independently of a COMPUTE instance.
* Reconciling commands is the mechanism to match a COMPUTE instance's state to what the COMPUTE controller expects after startup.
* On startup, the COMPUTE controller provides the current configuration to the COMPUTE instances.
* COMPUTE instances can match the configuration to their own state and only apply changes.

For this purpose, we interpret the commands COMPUTE controller provides to COMPUTE as an append-only log.
Upon COMPUTE controller restart, the new configuration is/should be a suffix of the log of all previously received commands.

The log can be compacted by collapsing commands that semantically supersede previous commands.

The `ComputeCommand` protocol should provide enough information to fully reconcile the command history.
Each `CreateInstance` command serves as the punctuation to truncate and restart the command log.

### State

We assume that the mapping of `GlobalId` to an object is unique, i.e., after restarts they still name the same object.
This allows us to maintain a small amount of state to enable command reconciling.

* For each installed identifier, CR maintains its upper frontier.
* CR retains a copy of dataflow plans to detect plan changes.
* CR maintains the set of created instances.

The controller sends commands that implicitly start tracking an upper frontier, each identified by a `GlobalId`.
If the command reconciliation is not yet tracking the frontier for a specific identifier, it starts tracking it.
If it is already tracking the frontier, it needs to update the controller about past progress, because the controller assumes that the frontier tracking starts at the minimum timestamp.
Afterwards, the frontier tracking continues as usual.

### Interpreting commands and responses

The command protocol exposes the following verbs to communicate state updates from the controller to a compute instance.
For each, we describe the associated action applied by CR.
* `CreateInstance`: If the instance is unknown, remember it and forward the command.
  Start tracking the frontiers of the logging dataflows if enabled.
* `DropInstance`: Forget the instance, stop tracking all associated frontiers.
* `CreateDataflows`: For each dataflow, start tracking the frontiers of all items it exports.
  Lookup existing dataflow by its `GlobalId` (see [Quirks](#quirks)), and remember the dataflow if it is new.
  If the identifier is bound, assert that the existing dataflow is compatible with the new definition.
  Dataflows are considered compatible when the imports, plan, and exports are equal and the `as_of` of the existing dataflow is not in advance of the new `as_of`.
  Forward the subset of new dataflows.
* `AllowCompaction`: Stop tracking an upper frontier if the controller permits compaction to the empty frontier.
  Forward command as-is.
* `Peek`: Remember active peek, forward command.
* `CancelPeeks`: Remove cancled peeks from active peeks, forward command.

CR handles responses similarly:
* `FrontierUppers`: Update the maintained frontier from the provided change batch and return the effective changes.
* `PeekResponse`: If there is an active peek, return the response.
* `TailResponse`: Pass through.

### Quirks

The implementation of the command reconciliation (CR) applies the following shortcuts, which should be handled better in the future:

* CR wants identifies plans by a single `GlobalId`, which only works for dataflows exporting a single index.

### Open questions

* The controller does not indicate the set of known IDs.
  This makes it difficult to determine which objects to uninstall once a controller reconnects.
  * The IDs are partially-ordered, which would allow the controller to indicate the lower bound of not-in-use IDs.
* How to handle inserts at a time <= the current object's frontier?
  We could just discard the data as it would probably be the same information already inserted earlier.
* When reconnecting, we have to get the controller's state up-to-date.
  The controller starts with a frontier of `[0]` when creating objects.
  STORAGE and controller should agree on a frontier before telling COMPUTE about it.
* The controller assumes that dataflows are rendered with a specific initial time.
  When restarting, we do not render previously rendered dataflows, which might violate the implicit contract expected by the controller.
  The reconciler tries to bring the controller up-to-date by replying with a correcting `FrontiersUppers` response.
  There might be a brief moment in time when the two are not synchronized and the controller could produce invalid commands.
  Is this a correct way of reconciling dataflows or do we need to drop and reconstruct?
  * In different world, the controller might create dataflows at `now` instead of the minimum time, which should always be in advance of the current dataflow's frontier.

## Storage command reconciliation

TODO: This section needs expanding.

Currently, the `StorageCommand` protocol does not express enough information to provide full command reconciliation.
* Materialize initiates tables with ephemeral information.
  For example, `show databases` reads from a table that is unconditionally initialized with `materialize`.
  The reconciler has no good mechanism to distinguish initialization updates from others.
    * *Current solution:* The storage command reconcile truncates all tables that saw inserts on `CreateInstance`.
* We need to validate if STORAGE's timestamp bindings are safe to be stored in memory within the instance until a controller picks them up.
* The commands supplied by the controller should be idempotent, but in fact are not.
  For example, when reading a Kafka topic, we create a new topic name each time we instantiate the source.
  This is incorrect because we bind an existing identifier to a new definition.

### Interpreting commands

The command protocol exposes the following verbs to communicate state updates from the controller to a storage instance.
For each, we describe the associated action applied by CR.
* `CreateSources`: For each source, start tracking its frontier. Lookup existing sources by its `GlobalId` and remember
  the source if it is new. If the identifier is bound, assert that the existing source is identical to the new
  definition.
* `AllowCompaction`: Stop tracking a source if the controller permits compaction to the empty frontier.
  Forward command as-is, since this command is idempotent.
* `Append`: Filter out all appends to a source that have a timestamp less than the source's frontier. Forward the rest.
  * NOTE: as discussed above, this doesn't currently work for all appends, specifically initialization appends.

## Response reconciliation

TODO: This section needs expanding.

Responses represent results and data a COMPUTE instance provides the ADAPTER.
* Multiple COMPUTE instances can appear as a single unit outwards.
* Response reconciliation ensures that responses from many COMPUTE instances appear as if it was a single instance.
