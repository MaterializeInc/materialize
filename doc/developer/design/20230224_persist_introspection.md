- Feature name: Persist Introspection Sources
- Associated: [13918](https://github.com/MaterializeInc/materialize/issues/13918)

# Summary
[summary]: #summary

A SQL-level representation of the persist _State_ metadata for a shard. The UX of these is heavily inspired by compute's introspection sources. See [the demo].

[the demo]: https://materializeinc.slack.com/archives/CNVRXGFDJ/p1676675501147309

# Motivation
[motivation]: #motivation

- Primarily: Debugging. The `persistcli` tools allow for introspection of the same data but they're hard to use (require prod access and have fiddly command-line arguments) and each one requires writing and committing new rust code. Exposing the raw information in SQL makes ad-hoc analysis much easier as well as makes it available to our continuous view maintenance engine.
- Secondarily: Testing. Exposing persist internals allows testdrive and other SQL tests to make assertions about the state of the persist shards powering the system. These are things that should be transparent to the user, but in practice it's useful to be able to write assertions about e.g. how much space a shard is using.
- Speculatively: In the future, these might power user-facing introspection tools. For example: real-time breakdowns of storage usage or information about frontier propagation delays in chains of materialized views.

# Explanation
[explanation]: #explanation

Persist already structures its internal State metadata as a set of differential dataflow collections (currently 7 of them). Persist necessarily writes these down durably in the course of normal operation, so they are already _definite_. They are naturally keyed by _SeqNo_ (an identifier for versions of State as it changes over time). However, we also tag each one with a strictly increasing wall-clock timestamp, which makes them also _reclocked_ collections.

If we then map each of them to a `RelationDesc`, we have everything we need to use them as inputs to dataflows in compute, making them usable in `SELECT`s, `INDEX`es, and `MATERIALIZED VIEW`s.

This results in a timely operator that takes a `ShardId` and returns the following struct (TODO: or a single output of an `enum` type instead?):

```rust
pub struct PersistMetadata<G: Scope> {
    // RelationDesc::empty()
    //   .with_column("ts", ScalarType::UInt64.nullable(true))
    pub since: Collection<G, Row, Diff>,
    // RelationDesc::empty()
    //   .with_column("lower", ScalarType::UInt64.nullable(true))
    //   .with_column("upper", ScalarType::UInt64.nullable(true))
    //   .with_column("since", ScalarType::UInt64.nullable(true))
    //   .with_column("encoded_size_bytes", ScalarType::UInt64.nullable(false))
    pub batches: Collection<G, Row, Diff>,
    // RelationDesc::empty()
    //   .with_column("id", ScalarType::String.nullable(false))
    //   .with_column("since", ScalarType::UInt64.nullable(true))
    //   .with_column("seqno", ScalarType::UInt64.nullable(false))
    //   .with_column("last_heartbeat_timestamp_ms", ScalarType::UInt64.nullable(false))
    //   .with_column("hostname", ScalarType::String.nullable(false))
    //   .with_column("purpose", ScalarType::String.nullable(false))
    pub leased_readers: Collection<G, Row, Diff>,
    // RelationDesc::empty()
    //   .with_column("id", ScalarType::String.nullable(false))
    //   .with_column("since", ScalarType::UInt64.nullable(true))
    //   .with_column("opaque", ScalarType::UInt64.nullable(true))
    //   .with_column("hostname", ScalarType::String.nullable(false))
    //   .with_column("purpose", ScalarType::String.nullable(false))
    pub critical_readers: Collection<G, Row, Diff>,
    // RelationDesc::empty()
    //   .with_column("id", ScalarType::String.nullable(false))
    //   .with_column("most_recent_write_upper", ScalarType::UInt64.nullable(false))
    //   .with_column("last_heartbeat_timestamp_ms", ScalarType::UInt64.nullable(false))
    //   .with_column("hostname", ScalarType::String.nullable(false))
    //   .with_column("purpose", ScalarType::String.nullable(false))
    pub writers: Collection<G, Row, Diff>,
}
```

They are exposed in SQL as follows (strawman, requesting takes on this):
- `SELECT * FROM PERSIST BATCHES METADATA FROM name_of_source_or_materialized_view`
- This is only usable by the internal `mz_introspection`/`mz_system` users as well as by the system itself.
- `BATCHES` can be one of several tokens, which each correspond to one of the persist metadata collections and a different `RelationDesc`.
- Open question: This uses the "data" shard of the named item. Do we also want to expose the "remap" shard? What about other shards in the system?
- Rejected alternative: We could also make the persist introspection source a named catalog time like we (will) do for exposing the reclock information. The overhead of this would be too high.

# Reference explanation
[reference-explanation]: #reference-explanation

TODO. For now a series of open questions:
- Baking the reclocking into persist as it writes each new version of State makes it essentially free, but introduces assumptions about the timeline. Do we care? Alternatives like using our normal reclocking logic seem too heavyweight.
- How will we handle the "since" of these collections/the "as_of" of the operator? It's possible for persist to learn to hand out a capability on the timestamp of these collections, which will hold back persist GC appropriately, but where does this go in the controller?
  - Aside: These timestamp capabilities could be an interesting primitive in a later feature to support reading the data in a persist shard with _only read-only S3 access_.
- How will we represent these in the controller and compute `*IR`s?
- How will dataflow rendering of these work?

# Rollout
[rollout]: #rollout

## Testing and observability
[testing-and-observability]: #testing-and-observability

The timely operator that generates these collections will be tested using rust unit tests, as all persist code is. The mapping from the internal persist `StateDiff` to `Row` will be tested using sqllogictest and/or testdrive.

## Lifecycle
[lifecycle]: #lifecycle

These will initially be shipped behind a feature flag gate, which will not be enabled on production user environments. If they prove to be sufficiently interesting, we will schedule follow-up work to make them more robust as a separate work item.

# Drawbacks
[drawbacks]: #drawbacks

Exposing such internal persist details at higher levels leaks the abstraction. It also might slow down otherwise-internal persist PRs if we have to consider catalog migrations referencing these introspection sources.

# Conclusion and alternatives
[conclusion-and-alternatives]: #conclusion-and-alternatives

Inline above.

# Unresolved questions
[unresolved-questions]: #unresolved-questions

Inline above.

# Future work
[future-work]: #future-work

Inline above.
