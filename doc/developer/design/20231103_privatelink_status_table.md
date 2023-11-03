# PrivateLink Connection Status History Table

### Associated:
- https://github.com/MaterializeInc/materialize/issues/19022
- https://github.com/MaterializeInc/cloud/issues/5190
- https://github.com/MaterializeInc/materialize/pull/20681
- https://github.com/MaterializeInc/cloud/pull/6383


## The Problem

Configuring an AWS PrivateLink connection is often one the first technical
interactions a customer has with Materialize, and can be  difficult to debug
when set up incorrectly. The initial setup process encompasses many states
and might involve manual approval of the connection request by the customer.

Currently, Materialize allows a user to validate a PrivateLink connection
using the `VALIDATE CONNECTION` command, which returns an error and
user-facing message if the connection does not have an `available` state.
This provides a basic debug tool for users to understand the present state
of each connection, but doesn't provide an auditable history of connection
state changes over time.

To reduce user-experienced friction during the configuration process and to
log and diagnose failed PrivateLink connections after initial setup,
this document proposes a new system table to record the history of state
changes of each AWS PrivateLink connection.


## Success Criteria

Users should be able to access an `mz_internal` table that records the state
changes of each of their PrivateLink connections, based on the state exposed
on the VpcEndpoint for each connection.


## Solution Proposal

Add a new table to `mz_internal`:

**mz_aws_privatelink_connection_status_history**

| Field             | Type                       | Meaning                                                    |
|-------------------|----------------------------|------------------------------------------------------------|
| `connection_id`   | `text`                     | The unique identifier of the AWS PrivateLink connection. Corresponds to `mz_catalog.mz_connections.id`   |
| `status`           | `text`                     | The status: one of `pending-service-discovery`, `creating-endpoint`, `recreating-endpoint`, `updating-endpoint`, `available`, `deleted`, `deleting`, `expired`, `failed`, `pending`, `pending-acceptance`, `rejected`, `unknown`                        |
| `occured_at`      | `timestamp with time zone` | The timestamp at which the state change occured.       |

The events in this table will be persisted via storage-managed collections,
rather than in system tables, so they won't be refreshed and cleared on
startup. The table columns are modeled after `mz_source_status_history`.

The `CloudResourceController` will expose a `watch_vpc_endpoints` method
that will establish a Kubernetes `watch` on all `VpcEndpoint`s in the
namespace and translate them into `VpcEndpointEvent`s (modeled after
the `watch_services` method on the `NamespacedKubernetesOrchestrator`)

where `VpcEndpointEvent` is defined as follows:

``` rust
struct VpcEndpointEvent {
    connection_id: GlobalId,
    vpc_endpoint_id: String,
    status: VpcEndpointState,
    transition_time: DateTime,
}
```

This `watch_vpc_endpoints` method will maintain an in-memory map of the last
known state value for each `VpcEndpoint`, compare that to any received
Kubernetes watch event, and upon detecting changes emit a `VpcEndpointEvent`
to the returned stream.

The `transition_time` field will be determined by inspecting the `Available`
"condition" on the `VpcEndpointStatus` which contains a `last_transition_time`
field populated by the VpcEndpoint Controller in the cloud repository.

The `status` field will be populated using the `VpcEndpointStatus.state` field.

The adapter `Coordinator` (which has a handle to `cloud_resource_controller`)
will spawn a task on `serve` (similar to where it calls
`spawn_statement_logging_task`) that calls `watch_vpc_endpoints` to
receive the stream of `VpcEndpointEvent`s.

This task will translate received `VpcEndpointEvent`s into `Row`s and buffer
these in a vector. On a defined interval (e.g. 5 seconds) it will issue a
coordinator message via `internal_cmd_tx` to flush the recorded events
to storage using the `StorageController`'s `record_introspection_updates`.
This is modeled after `StatementLogging::drain_statement_log` to avoid
unnecessary load on cockroach if there is a spike of received events.


## Alternatives

1. Poll the `list_vpc_endpoints` method on a defined interval rather than
   spawning a new task to listen to a kubernetes watch. This would have a more
   consistent performance profile, but could make it possible to miss state
   changes. With a kubernetes watch we will receive all VpcEndpoint updates
   which could be noisy if an endpoint were to change states at a high-rate.
   Since we will be buffering the writes to storage, this seems unlikely to be
   problematic in the current design.

2. Use an ephemeral system table rather than persisting via a storage-managed
   collection. This history seems most useful to persist long-term, as the
   state changes do not occur frequently once a connection has been
   successfully established. This also matches the semantics of the
   `mz_source_status_history` and similar tables.


## Open questions

1. We are likely to record duplicate events on startup, since the
   `watch_vpc_endpoints` method won't know the 'last known state' of each
   `VpcEndpoint` recorded into the table.
   
   We could use the `last_transition_time` on the `Available` condition in
   the `VpcEndpointStatus` to determine if this transition happened prior to
   the Adapter wallclock start-time. However this might cause us to miss a
   state change if it was not written to the table during the previous database
   lifecycle.

   Is it better to duplicate rows on startup, or potentially miss events that
   occur between environmentd restarts?

2. Upon inspecting all the existing `VpcEndpoint`s in our `us-east-1` cluster
   I noticed that they all had the exact same timestamp in the
   `last_transition_time` field on their `Available` condition. This seems odd
   so we should confirm that this field is being updated appropriately.

3. Do we need to buffer events? Instead we could write to storage on each event
   received. Since we don't expect to receive a high-frequency of events it's
   unclear if the buffering is as necessary as it is with statement logging.
   Without the buffering we are less likely to drop a new event received right
   before environmentd shutdown.
