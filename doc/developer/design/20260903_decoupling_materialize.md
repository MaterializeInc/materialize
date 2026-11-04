# Decoupling Materialize

## Architectural outcome

Separate deciding what should run, managing its processes, and executing database
work. An adapter's lifetime, or its connection to a replica, should not determine
the lifetime of maintained storage and compute.

The adapter-owned lifecycle protocol couples these responsibilities. The adapter
holds desired state, sends installation and compaction commands, and restores
that state over replacement connections. A replica's continued execution is
therefore entangled with the adapter's connection and recovery machinery.

This design removes that lifecycle connection. Replicas recover and follow
durable intent independently. Adapters still communicate directly with replicas
for requests such as peeks, but those connections own requests, not maintained
dataflows. Reconnecting restores request access rather than replaying the
database's lifecycle.

The architectural benefit is narrower failure boundaries and fewer intertwined
state machines. Horizontal adapter scaling, high availability and upgrades that
preserve running work follow from this separation. They are described in
[Capabilities and consequences](20260903_decoupling_materialize_additional.md).
The [detailed design](20260903_decoupled_coordination.md) specifies the boundary
contracts and milestone scope. This overview is not an implementation-status
report.

## Responsibilities

| Component | Responsibility |
| --- | --- |
| Adapters (`adapterd`) | Plan requests, commit object definitions and plans, decide replica-set policy, and execute client requests. |
| Deployment controller (`controllerd`) | Initialize deployment infrastructure and reconcile committed process inventory through an orchestration backend. |
| Replicas (`clusterd`) | Follow the catalog, install and maintain storage and compute, and serve requests. |
| External lifecycle owner | Launch controllers, authorize promotion and retirement, manage routing, and finish deployment teardown. |
| Shared relay (`pubsubd`) | Accelerate discovery of durable Persist changes without owning their contents or authority. |

```text
  adapters ---- commit ----> catalog ---- inventory ----> controllerd
      |                        ^    |                         |
      |              progress, |    | definitions,             | provisions
      |             protection |    | plans, authority         v
      |                        |    v                adapter / replica processes
      +---- fast requests ---> replicas
      <------- results --------+

  Shared Persist state provides durable communication.
  Pubsub accelerates notifications, with durable fallback.
```

Policy and provisioning are separate. An adapter may decide that a cluster needs
another replica, but it commits that decision before controllerd starts the
process. Replicas do not wait for controllerd to install dataflows. One controller
manages each deployment identity, allowing old and new deployments to retain
their matching process configuration.

Controllerd is not a replacement singleton for database progress. Eligible
adapters cooperate on duties such as table ticking and shard finalization.
Replica observations belong to replicas. These responsibilities must remain safe
with several participants, rather than depend on a designated adapter.

## Communication and coordination

### Durable intent, not a persisted command queue

The catalog is the authority for maintained definitions, selected plans,
deployment membership, execution authority and permission to discard history.
Participants commit changes and independently follow the committed state.
A recovering participant can reconstruct what must exist without finding the
process that originally requested it.

This is not the old lifecycle protocol stored in a queue. Durable state describes
the required result, not an adapter's sequence of commands. Each participant
reconciles the part it owns. Progress and protection flow back through the same
authority where coordination is required. User data retains its existing Persist
and transaction-WAL write protocols, rather than moving into the catalog.

Pubsub makes following durable changes cheaper and faster. Notifications are not
commits, and relay availability grants no authority. Reconnection and missed
notifications use the durable fallback.

### Fast, request-scoped execution

Peeks and query-local dataflows, including SELECTs, SUBSCRIBEs and COPY TO, use a
separate fast protocol. Their responses, cancellation and cleanup belong to the
requesting client. A new client must not replace another client's desired state,
and losing a client must not tear down maintained execution.

Durability is useful for intent that must outlive its sender. It is not required
for every transient request or local read-hold change. The fast path preserves
that distinction without becoming an alternative lifecycle authority.

### Contracts between the paths

**Ordering:** catalog commit, replica application and query readiness are distinct.
A request carries the catalog position covering its validated dependencies.
The receiver waits for the required state to be applied before execution.
Independently serving adapters also establish catalog freshness before relying
on definitions, permissions and configuration.

**Protection:** observing a frontier is not acquiring a read hold. Reads secure
protection before using history. Committed compaction permission accounts for
client reads, maintained recovery and retention across participants. Applying
that permission may lag, but cannot exceed it. Local aggregation avoids a durable
write for every query hold change.

**Authority:** concurrent decisions validate their preconditions at catalog commit,
not only against a preceding local snapshot. Replacement and retirement revoke
specific executions or deployments without fencing unrelated participants.
Output-specific write protocols enforce authority where concurrent enactment
could otherwise corrupt results.

## Failure boundaries and recovery

| Failure | Boundary |
| --- | --- |
| Adapter loss | Its requests and sessions may fail. Peers and maintained execution continue. |
| Controller loss | Healthy children continue. Provisioning and failed-process recovery may wait. |
| Replica loss | Its execution is interrupted. A replacement reconstructs from durable intent and protected inputs. |
| Relay loss | Durable fallback preserves progress, with potentially higher latency and backend traffic. |

With zero adapters, replica-set policy and adapter-owned maintenance may pause.
Decoupling does not make every component dispensable or mask every failure.
Shared durable services remain dependencies, and transparent session failover is
not part of this design.

Deployment handover uses the same separation. A compatible deployment joins and
warms its own replicas while the existing deployment serves. Promotion transfers
active authority without discarding warmed execution. Retirement is a separate
decision, not a consequence of startup or promotion. Shared formats and write
semantics must remain compatible for all admitted participants.

The tradeoff is explicit durable coordination: catalog traffic, contention and
conservative history retention replace connection-coupled lifecycle management.
The objective is not to eliminate coordination, but to put it at boundaries that
survive failure and can be understood independently.
