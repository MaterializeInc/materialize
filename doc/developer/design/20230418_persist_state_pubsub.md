- Feature name: Persist State PubSub
- Associated: https://github.com/MaterializeInc/materialize/issues/18661

# Summary
[summary]: #summary

Complementing Persist's polling-based discovery of new state with pub-sub updates.

# Motivation
[motivation]: #motivation

tl;dr
* Reduces CockroachDB read traffic to near zero, reducing current CPU usage by 20-30% (relative to total usage).
  This will allow us to [scale down our clusters further](https://github.com/MaterializeInc/materialize/issues/18665).
* Reduces the median latency of observing new data written to a shard from ~300ms to single digit millis

---

Full context:

Persist stores the state related to a particular shard in `Consensus`, which today is implemented on CockroachDB (CRDB).
When any changes to state occur, such a frontier advancement or new data being written, a diff with the relevant change
is appended into `Consensus`. Any handles to the shard can read these diffs, and apply them to their state to observe
the latest updates.

However, today, we lack a way to promptly discover newly appended diffs beyond continuous polling of `Consensus`/CRDB.
This means that ongoing read handles to a shard, such as those powering Materialized Views or `Subscribe` queries, must
poll `Consensus` to learn about progress. This polling today contributes ~30% of our total CRDB CPU usage, and introduces
~300ms worth of latency (and ~500ms in the worst case) between when a diff is appended into `Consensus` and when the read
handles are actually able to observe the change and progress their dataflows.

If Persist had a sidechannel to communicate appended diffs between handles, we could eliminate nearly the full read load
we put on CRDB, in addition to vastly reducing the latency between committing new data and observing it. This design doc
will explore the interface for this communication, as well as a proposed initial implementation based on RPCs that flow
through `environmentd`.

# Explanation
[explanation]: #explanation

## PubSub Trait

We abstract the details of communicating state diffs through a Trait that uses a publisher-subscriber-like model. We aim
to keep the abstraction simple: callers are given an address that they can dial to begin pushing and receiving state diffs,
and that is all that is needed. This API is modeled in a way to set us up for future changes in implementation -- as we
scale, we may wish to back the sidechannel with an external pubsub/message broker system, such as Kafka, and this API
should be sufficient to do so.

One of the goals of the Trait is to encapsulate all of the logic needed to communicate diffs to the right parties within
Persist. The sidechannel used to communicate updates can be thought of as an internal detail to Persist: somehow, without
needing to know exactly how, Persist shards are able to update themselves in response to changes made in other processes,
and it does not need to be tightly bound to the coordination logic elsewhere in Materialize.

Below is a strawman proposal for this interface:

```rust
#[async_trait]
pub trait PersistPubSubClient {
  /// Receive handles with which to send requests and receive diffs.
  async fn connect(
    addr: String,
  ) -> Result<(Arc<dyn PubSubSender>, Box<dyn PubSubReceiver>), anyhow::Error>;
}

/// The send-side client to Persist PubSub.
pub trait PubSubSender: std::fmt::Debug + Send + Sync {
  /// Push a diff to subscribers.
  fn push(&self, shard_id: &ShardId, diff: &VersionedData);

  /// Subscribe the corresponding [PubSubReceiver] to diffs for the given shard.
  /// This call is idempotent and is a no-op for already subscribed shards.
  fn subscribe(&self, shard: &ShardId);

  /// Unsubscribe the corresponding [PubSubReceiver] to diffs for the given shard.
  /// This call is idempotent and is a no-op for already unsubscribed shards.
  fn unsubscribe(&self, shard: &ShardId);
}

/// The receive-side client to Persist PubSub.
pub trait PubSubReceiver: Stream<Item = ProtoPubSubMessage> {}
```

## Publishing & Applying Updates

When any handle to a persist shard successfully compares-and-sets a new state, it will publish the diff to `PubSubSender`.
Publishing the diff is not required for correctness in case the process crashes, hits a network partition, or otherwise
chooses not to publish it (e.g. feature flag).

When a process receives a diff via `PubSubReceiver`, it needs to apply it to the local copy of state and notify any readers
that new data / progress may be visible. The mechanisms to do this have been built:

* [#18488](https://github.com/MaterializeInc/materialize/pull/18488) gives each process a single, shared copy of persist
  state per shard. This gives us a natural entrypoint to applying the diff.
* [#18778](https://github.com/MaterializeInc/materialize/pull/18778) introduces the ability to `await` a change to state,
  rather relying on polling.

It is not assumed for message delivery to be perfectly reliable nor ordered. We will continue to use polling as a fallback
for discovering state updates, though with a more generous retry policy than we currently have. If a process receives a
diff that does not apply cleanly (e.g. out-of-order delivery), the process will fetch the latest state from Cockroach
directly instead. While we will continue to have these fallbacks, we anticipate message delivery being effective enough
to eliminate the need for nearly all Cockroach reads.

# Reference Explanation

As a first implementation of the Persist sidechannel, we propose building a new RPC service that flows through `environmentd`.
The service would be backed by gRPC with a minimal surface area -- to start, we would use a simple bidirection stream that
allows each end to push updates back and forth, with `environmentd` broadcasting out any changes it receives.

## RPC Service

```protobuf
message ProtoPushDiff {
  string shard_id = 1;
  uint64 seqno = 2;
  bytes diff = 3;
}

message ProtoSubscribe {
  string shard = 1;
}

message ProtoUnsubscribe {
  string shard = 1;
}

message ProtoPubSubMessage {
  oneof message {
    ProtoPushDiff push_diff = 1;
    ProtoSubscribe subscribe = 2;
    ProtoUnsubscribe unsubscribe = 3;
  }
}

service ProtoPersistPubSub {
  rpc PubSub (stream ProtoPubSubMessage) returns (stream ProtoPubSubMessage);
}
```

The proposed topology would have each `clusterd` connect to `environmentd`'s RPC service. `clusterd` would subscribe to
updates to any shards in its `PersistClientCache` (a singleton in both `environmentd` and `clusterd`). When any `clusterd`
commits a change to a shard, it publishes the diff to `environmentd`, which tracks which connections are subscribed to which
shards, and broadcasts out the diff to the interested subscribers. From there, each subscriber would [apply the diff](#applying-received-updates).
Note that a `clusterd` would only subscribe to a shard if it is actively reading it, e.g. for a materialized view, `SELECT`,
or a `SUBSCRIBE`, so the number of broadcasted messages would be proportional to the number of ongoing read operations,
and not the total number of shards.

`environmentd` is chosen to host the RPC service as it necessarily holds a handle to each shard (and therefore is always
interested in all diffs), and to align with the existing topology of the Controller, rather than introduce the novel
complexity of point-to-point connections between unrelated `clusterd` processes or a net-new dedicated process. With this
implementation we would anticipate the end-to-end latency between a writer in one `clusterd` pushing an update to a reader
in another `clusterd` receiving that update to be in the 1-10ms range.

### Service Discovery

`clusterd` will need to know the address of the `environmentd` process in order to connect to the PubSub server. Locally,
this will be done through a standard `127.0.0.1:6879` port and passed in to `clusterd` via command line argument.

In Kubernetes, we will create a new headless `Service` in the `environment-controller` that routes to the pubsub port
(likely 6879) of the current generation of `envd`:

```yaml
apiVersion: v1
kind: Service
metadata:
  labels:
    materialize.cloud/environment-name: env-name
    materialize.cloud/organization-id: org-name
  name: persist-pubsub-<envd-generation>
  namespace: env-name
spec:
  selector:
    materialize.cloud/app: environmentd
    # ensure this service only routes to a specific generation of `envd`
    materialize.cloud/envd-generation: <envd-generation>
  type: ClusterIP
  clusterIP: None
  ports:
  - name: pubsub
    port: 6879
    protocol: TCP
    targetPort: 6879
```

`envd` will be informed of this service name via command line argument, e.g. `--persist-pubsub-service-name persist-pubsub-1`
and pass it along to its `clusterd` param when set.

We introduce this new headless service, rather than reusing the existing `environmentd` Service for a few reasons:

* The `environmentd` Service is public / external-facing, while the pubsub port should be strictly internal
* To decouple the idea the notion that `environmentd` is necessarily responsible for providing the PubSub server. In the
future we may wish to [replace it with a different implementation](#external-pubsubmessage-bus), or route the Service to
point towards different pods.

We will create the Service per `envd` generation, so that `clusterd` will always be routed to the `envd` matching their
generation, and that multiple generations may coexist.

# Rollout
[rollout]: #rollout

We will roll out this change in phases behind a feature flag. The change is seen purely as an optimization opportunity,
and should be safe to shut off at any time. We will aim to ensure that any errors handled due to Persist PubSub cannot
crash or halt the process.

We anticipate message volume to be on the order of 1-1.5 per second per shard, with each message in the 100 bytes-1KiB
range. Given the scale of our usage today, we would expect `environmentd` to be able to broadcast this data and message
volume comfortably with minimal impact on performance.

That said, we'll want to keep an eye on how much time `environmentd` spends broadcasting diffs, as it will be a new
workload for the process. If `environmentd` CPU usage spikes too high (e.g. >0.5-1 CPU), we will reconsider the approach
entirely. Early prototyping has indicated a very small rise in CPU (0.05-0.1 range for a heavy workload).

We anticipate `clusterd` to be negligibly impacted by this added RPC traffic, as each `clusterd` would only need to
push/receive updates for the shards it is reading and writing, work is that is very similar to what it is doing today.

Our existing Persist metrics dashboard, in addition to the new metrics outlined below, should be sufficient to monitor
the change and understand its impact.

## Testing and observability
[testing-and-observability]: #testing-and-observability

We will introduce several metrics to understand the behavior of the change:

* Number of updates pushed
* Number of updates received that apply cleanly & uncleanly
* How frequently `Listen`s need to fallback to polling
* End-to-end latency of appending data to observing it
* Timings of `environmentd`'s broadcasting / load

CI will be used to ensure we do not introduce novel crashes or halts, but we anticipate the most interesting data to
come from opted-in environments in staging and prod, rather than what we can synthetically observe.

## Lifecycle
[lifecycle]: #lifecycle

We will use a feature flag that can quickly toggle on the publication / subscription to updates dynamically. Our metrics
dashboards will inform our rollout process.

# Drawbacks
[drawbacks]: #drawbacks

Looking forward, we know our polling-based discovery is not long-term financially sustainable and will need replacement.
The latency incurred by polling reduces the interactive experience of the product, and polling read cost multiplies more
quickly than write cost, scaling with the number of materialized views / subscribes and not just with shard count. An empty
environment has write-dominant costs, but our largest and most active environments have read-dominant costs as their usage
has grown, in a way that has outsize impact on our total CRDB usage.

In terms of the implementation suggested, we will introduce another workload to `environmentd` in maintaining Persist
PubSub connections and broadcasting messages. While we think the overhead will be low, and scalable to a large number
of shards even with `environmentd`'s baseline resources, it does add more responsibility and load to the critical service.

Additionally, while we have designed Persist PubSub to be an optimization and not required to operate each process, the
optimization could become load-bearing if CRDB is proportionally scaled down (which realistically is what is needed to
realize the cost-savings goal), barring us from easily toggling the feature off if problems arise. This is certainly a
concern, and the current mitigations are that: we already run CRDB with a comfortable enough headroom to accommodate
turning off / a failure in Persist PubSub; we can pre-scale CRDB relatively quickly to if we need to disable the feature.

Lastly, in terms of cost efficiency, one of the outcomes of this work is essentially exchanging CRDB CPU time for `envd`
resources in a way that we anticipate being advantageous, but could turn out to be misguided: e.g. perhaps we can reduce
CRDB CPU by 30% but each `envd` suddenly requires 2 more CPUs, and our infra bill increases as a result. However, early
prototyping here indicates that Persist PubSub is _(handwaving intensifies)_ somewhere on the order of 25x-50x more CPU
efficient than our polling approach, when comparing the rise in `envd` CPU seconds versus the drop in CRDB.

# Conclusion and alternatives
[conclusion-and-alternatives]: #conclusion-and-alternatives

## Architectural

As Materialize is currently architected, Persist serves as a type of communication layer between processes. A sidechannel
would allow for quicker, more efficient communication between processes access the same shard.

An alternative to this architecture, that would obsolete the need for a sidechannel entirely, would be to consolidate
interactions with Persist purely into `environmentd`, and allow it to instruct `clusterd` what work to perform explicitly.
Some ideas have been floating that [move in this direction](https://materializeinc.slack.com/archives/C04LEA9D8BU/p16813495590089590).

After much thought on this one, our determination is that, regardless of whether we make broader architectural changes,
a Persist sidechannel is worth pursuing now, even if it is obsoleted and ripped out later. It would meaningfully benefit
the product and our infra costs sooner rather than later, its scope is well contained, and it would not introduce any new
complexity that would impede any larger reworkings.

## Sidechannel Implementations

While we have proposed an RPC-based implementation that flows through `environmentd`, these are the alternatives we've
considered:

### Controller RPCs

We could push the RPCs into the Controller, and use the existing Adapter<->Compute|Storage interfaces to communicate diffs.
This would avoid any complexity involved in setting up new connections and request handling, and allow the Controller to
precisely specify which shards are needed by which processes, which is information that it already knows.

To this point, we've rejected this option for several reasons: Persist sidechannel traffic could become a noisy neighbor
(or by affected by noisy neighbors) in the existing request handling; pushing diff updates is purely an optimization while
other messages in the Controller RPC stream are critical for correctness and overall operations; Persist internal details
leak into the Controller in a way that weakens its abstraction.

### Point-to-Point RPCs / Mesh

We could alternatively set up the proposed gRPC service to connect `environmentd` and each `clusterd` to each other
directly, creating a mesh. In this model, `environmentd` would no longer need to broadcast out changes -- each process
would send diffs directly to the processes that need them.

This approach is additionally interesting in that it would set Persist up to communicate other types of data as well,
not just pub-sub of diffs. One hypothetical -- shard writers could cache recently-written blobs to S3 in memory or on
disk, and other processes could request those blobs from the writer directly, rather than paying the costs of going to
S3, which is likely to have much longer tail latencies.

The downsides of this approach include the complexity overhead, as it introduces the novel pattern of intra-`clusterd`
communication in a way that does not exist today, and the scaling limitations imposed by all-to-all communication.

### `clusterd`-hosted RPC services

The proposed topology has `environmentd` host a singular RPC service that each `clusterd` dials in to. An alternative
would be to reverse the direction, and have `environmentd` dial in to each `clusterd`, similar to how the Controller gRPC
connections are set up. This may be easier to roll out as the design is a known quantity, and `environmentd` can use its
knowledge of which `clusterd` are created and dropped to set up the appropriate connections.

We are partial to the proposed design however, as it more closely models accessing an external PubSub system: clients
are given an address to connect to, and begin publishing and subscribing to messages with no further information needed.
And if individual clients fail to connect, or choose not to (e.g. via feature flag), no harm done, everything continues
to work via fallbacks. Consolidating the discovery and connection establishment into `environmentd` breaks this abstraction
in a way that would make swapping implementations more challenging (e.g. Kafka wouldn't be dialing in to `clusterd`!),
making it harder for Persist to treat its PubSub implementation as an internal detail.

### External PubSub/Message Bus

The [PubSub Trait](#pubsub-trait) would be a good fit for an external message bus, like SQS, Kafka, RedPanda, etc, and
has been designed to accommodate such a choice long-term. These systems could easily support the workload we've outlined.
However, since we do not currently operate any of these systems, and because one of the primary impetuses for the work is
to cut down on infrastructure cost, it would both be a significant lift and added cost to introduce one of them. This
option may be worth exploring in the future as our usage grows, or if other product needs arise that demand a bonafide
message bus.

### CRDB Changefeeds

CRDB contains a CDC feature, Changefeeds, that would allow us to subscribe to any changes to our `Consensus` table.
While appealing on the surface, we do not believe Changefeeds provide the right tradeoffs -- one of our goals is to
reduce CRDB load; low latency delivery of updates [is not guaranteed or expected](https://github.com/cockroachdb/cockroach/issues/36289);
and the ability to scale to 100s or 1000s of changefeeds per CRDB cluster is unknown.

# Unresolved questions
[unresolved-questions]: #unresolved-questions

TBD, no obviously unresolved questions yet

# Future work
[future-work]: #future-work

Once we have the groundwork in place to push diffs, there will likely be other applications and optimization opportunities
we can explore, e.g. we could push data blobs (provided they are small) between processes to eliminate many S3 reads.
