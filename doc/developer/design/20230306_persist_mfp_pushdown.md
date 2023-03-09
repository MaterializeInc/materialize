- Feature name: Persist MFP Pushdown
- Associated: https://github.com/MaterializeInc/materialize/issues/12684

# Summary
[summary]: #summary

<!--
One paragraph to explain the feature: What's wrong, what's the problem, what's the fix, what are the consequences?
-->

Enabling read and cost optimization opportunities in Persist through foundational work to columnarize, understand, and summarize its data.

# Motivation
[motivation]: #motivation
<!--
Why are we doing this? What problems does it solve? What use cases does it enable? What is the desired outcome?
-->

## Faster Reads / Rehydration

Blobs written by Persist today are a blackbox, without an easy way to understand their contents without fully 
pulling down every part written to Blob storage and decoding each row. Read access is therefore naive, always 
requiring us to fetch every part no matter what question is being asked of the data. 

By introducing types and capturing statistic metadata on parts, Persist will have the primitives necessary to 
more selectively filter which parts are fed into a dataflow, before even reading them from Blob storage. And 
once the data is columnar, we will have a wide range of OLAP-style optimizations at our disposal to further 
improve performance.

When reading data with the appropriate locality, MFP pushdown will enable faster dataflow rehydration and 
faster ad-hoc / exploratory queries in ways that are not currently achievable.

## Logical Sharding

By enabling faster / more selective reads, we additionally open up higher-level ideas like logical sharding, 
where we store the data of many collections in a single physical shard. An example of this could be our existing 
system tables -- rather than dedicating a Persist shard to each one, we could use a single physical
shard and rely on MFP pushdown to reduce reads to (mostly) only fetch data for individual system tables.
Doing so could vastly reduce the cost of small shards, particularly in their load on CRDB and PUT cost to S3, 
with minimal impact on read performance.

## Schemas

Understanding schemas at the persistent data level gives us a building block [to explore schema evolution at
the Persist level](https://github.com/MaterializeInc/materialize/issues/16625).

## Internal Improvements

Less tangible motivation for this work is that by columnarizing and summarizing parts, we will have a forcing 
function to progress some lesser-seen concerns within Persist. Examples of this include how proper columnar 
storage will reduce allocations and memory pressure caused by Persist (something we're already grappling with), 
and how storing statistics will require a more long-term scalable solution to how we store metadata. These are
problems we know we must stay on top of, and working through the building blocks for MFP pushdown will help frame
a long-term vision to solving them.

# Explanation
[explanation]: #explanation

A large amount of the changes needed for MFP pushdown are within Persist, where we must first build in primitives
to understand and summarize the data we receive. The interface to this work relevant for other teams (Compute specifically)
[is noted below in the Reference explanation](#filtering-parts).

## Data Types

Persist will need to understand individual columns of data, which will require newly supporting data types.
These data types will be columnar encoded using Arrow/Parquet for their in-mem/durable representation,
and allow for aggregations (e.g. min and max calculations) over columns.

## Schemas

A `Schema` trait will be introduced to map data back and forth between Persist columns. Any Rust type
that is to roundtrip through Persist must have a corresponding `Schema`.

## Part Statistics

When Persist writes a batch to Blob storage, it will compute basic statistics over its columns, when possible,
and write this data alongside existing metadata in Consensus. To start, we will collect a column's 
non-null min and max values and null count, as well as the min/max Row within the part. Statistics will always 
be optional, as Persist may not be able to compute min/max values for all types under all situations.

## MFP

### Filtering

Before batch parts are fetched from Blob storage in `persist_source`, the dataflow's MFP is applied to 
each part's statistics to determine whether it contains data relevant to the dataflow. If not, we get to
skip fetching the part from Blob storage entirely.

Note that:
* A part may or may not have statistics for all of its columns
* The MFP's filter will need to be modified to apply to min/max ranges, rather than individual rows
* In the absence of sufficient statistics or an applicable filter, we default to fetching the part like
we currently do. 

### Projection

While out of scope for the initial work, we can additionally push down our MFP's projection by only
decoding the columns necessary for the dataflow. This is particularly valuable for datasets with wide
rows that typically see narrower reads.

## Filtering & Locality

Having column statistics is a necessary building block to filter out parts before fetching them, but is
not guaranteed to be effective in actual queries unless there is some degree of locality to the filtered 
column values across parts in Blob storage.

Out of the box, we'd expect the following filters to be effective:

* Temporal filtering on any columns that are correlated with realtime. This is due to the temporal locality
in how Persist ingests and compacts data over time (by virtue of its Spine internals) -- the newest, and 
typically smallest, parts will contain the most recent updates, allowing one to filter out the older/larger
parts and their data effectively. This filtering is particularly relevant for append-only use cases where
we may have accumulated considerable numbers of older, larger parts that are no longer relevant to the filter.
* Filtering on a prefix of the `(key, value)` columns. This is due to how parts within Persist are sorted 
by `(key, value, ts)` in column/lexicographic ordering. In practice, the default column order is unlikely to
be immediately useful for most collections, and column ordering would likely need to be user-specifiable
to take full advantage of this property.

## Migration / Backwards-Compatibility

Persist will start writing parts to Blob storage in a new format. The migration path is relatively
straightforward:

* Persist starts writing all new parts in the new format.
* Persist continues to read both formats. Because filtering and column statistics are always optional,
parts written in the old format are simply treated as having no statistics and are always fetched.
* Over time, compaction migrates data from the old format into the new format.

<!--
Explain the design as if it were part of Materialize and you were teaching the team about it.
This can mean:

- Introduce new named concepts.
- Explain the feature using examples that demonstrate product-level changes.
- Explain how it builds on the current architecture.
- Explain how engineers and users should think about this change, and how it influences how everyone uses the product.
- If needed, talk though errors, backwards-compatibility, or migration strategies.
- Discuss how this affects maintainability, or whether it introduces concepts that might be hard to change in the future.
-->

# Reference explanation
[reference-explanation]: #reference-explanation

<!--
Focus on the implementation of the feature.
This is the technical part of the design.

- Is it reasonably clear how the feature is implemented?
- What dependencies does the feature have and introduce?
- Focus on corner cases.
- How can we test the feature and protect against regressions?
-->

## Filtering Parts
[filtering parts]: #filtering-parts

Pushing down an MFP will require Persist to offer a new interface so that Compute can filter 
at the part level, rather than row level.

Below is a (strawman!) proposal for accessing the statistics within a part:

```rust
/// Provides access to statistics stored for each Persist part (S3 data blob).
///
/// Statistics are best-effort, and individual stats may be omitted at any
/// time, e.g. if persist cannot determine them accurately, if the values are
/// too large to store in Consensus, if the statistics data is larger than
/// the part, etc.
struct SourceDataPartStats<K: Codec, V: Codec> {
    // internal details, etc.
    stats: PartStats<K, V>,
    relation_desc: RelationDesc,
    temp_storage: RowArena,
}

impl<K: Codec, V: Codec> SourceDataPartStats<K, V> {
    /// The number of updates (Rows + errors) in the part.
    pub fn len(&self) -> usize {
        todo!()
    }

    /// The number of errors in the part.
    pub fn err_count(&self) -> usize {
        todo!()
    }

    /// The part's minimum value for the named column, if available.
    /// A return value of `None` indicates that Persist did not / was
    /// not able to calculate a minimum for this column.
    pub fn col_min<'a>(&'a self, name: &str) -> Option<Datum<'a>> {
        todo!()
    }

    /// (ditto above, but for the maximum column value)
    pub fn col_max<'a>(&'a self, name: &str) -> Option<Datum<'a>> {
        todo!()
    }

    /// The part's null count for the named column, if available. A
    /// return value of `None` indicates that Persist did not / was
    /// not able to calculate the null count for this column.
    pub fn col_null_count(&self, name: &str) -> Option<usize> {
        todo!()
    }

    /// A prefix of column values for the minimum Row in the part. A
    /// return of `None` indicates that Persist did not / was not able
    /// to calculate the minimum row. A `Some(usize)` indicates how many
    /// columns are in the prefix. The prefix may be less than the full
    /// row if persist cannot determine/store an individual column, for
    /// the same reasons that `col_min`/`col_max` may omit values.
    pub fn row_min(&self, row: &mut Row) -> Option<usize> {
        todo!()
    }

    /// (ditto above, but for the maximum row)
    pub fn row_max(&self, row: &mut Row) -> Option<usize> {
        todo!()
    }
}
```

We additionally define a new trait, `PartFilter` that is used to return whether a given part should
be fetched, using the above interface to source statistics data about the part. Pushing down an MFP
would mean implementing this trait, and passing it in to `persist_source`:

```rust
pub trait PartFilter<K: Codec, V: Codec> {
    fn should_fetch<S: SourceDataPartStats<K, V>>(&self, part: &S) -> bool;
}
```

# Rollout
[rollout]: #rollout

<!--
Describe what steps are necessary to enable this feature for users.
How do we validate that the feature performs as expected? What monitoring and observability does it require?
-->

We will be able to roll out the new columnarized data format separately from enabling MFP pushdown. Rolling out
the new data format is summarized above in the migration path, where we read in both formats and write out one.
The format we write data in can be wrapped behind a feature flag.

Afterwards, we can roll out MFP pushdown behind feature flags:

* Start with soft evaluation of part filtering in staging / prod, where we always fetch each part and confirm that
would-be-filtered-out parts do not contain data needed
* After building sufficient confidence, turn on actual part filtering

We will want a long incubation cycle on the new format, rolling out to individual test accounts and staging
long before writing data in prod.

## Testing and observability
[testing-and-observability]: #testing-and-observability

Because the durable data format we use is so core to Materialize, a large amount of testing the new format will be 
done through existing tests. Within Persist we can additionally write:

* Proptests that verify all Datums can roundtrip through the new format
* Proptests to verify that all column types that we support statistics on sort equivalently to their Datum counterparts
* Benchmarks to verify the new format's performance in reads and writes
* Tests that verify the ability to read both formats simultaneously

Persist already has considerable metrics coverage and our existing dashboards will be crucial to monitoring the
behavior processes reading and writing the new format.

<!--
Testability and explainability are top-tier design concerns!
Describe how you will test and roll out the implementation.
When the deliverable is a refactoring, the existing tests may be sufficient.
When the deliverable is a new feature, new tests are imperative.

Describe what metrics can be used to monitor and observe the feature.
What information do we need to expose internally, and what information is interesting to the user?
How do we expose the information?

Basic guidelines:

* Nearly every feature requires either Rust unit tests, sqllogictest tests, or testdrive tests.
* Features that interact with Kubernetes additionally need a cloudtest test.
* Features that interact with external systems additionally should be tested manually in a staging environment.
* Features or changes to performance-critical parts of the system should be load tested.
-->

## Lifecycle
[lifecycle]: #lifecycle

<!--
If the design is risky or has the potential to be destabilizing, you should plan to roll the implementation out behind a feature flag.
List all feature flags, their behavior and when it is safe to change their value.
Describe the [lifecycle of the feature](https://www.notion.so/Feature-lifecycle-2fb13301803b4b7e9ba0868238bd4cfb).
Will it start as an alpha feature behind a feature flag?
What level of testing will be required to promote to beta?
To stable?
-->

As noted above, we can use feature flags to:

* Toggle which data format we write in
* Toggle whether MFP is: entirely disabled, soft evaluated, enabled

We will want long cycles in CI + staging before rolling out the new data format.

We will want a long period of data collection to ensure MFP pushdowns are accurately determining which parts to fetch.

# Drawbacks
[drawbacks]: #drawbacks

<!--
Why should we *not* do this?
-->

End-to-end completion of this work requires significant engineering investment. As we grow, Persist is near certain 
to need columnarized data _eventually_, and the bigger question is more about When the work is done, rather than If
the work is done.

# Conclusion and alternatives
[conclusion-and-alternatives]: #conclusion-and-alternatives

<!--
- Why is the design the best to solve the problem?
- What other designs have been considered, and what were the reasons to not pick any other?
- What is the impact of not implementing this design?
-->

Teaching Persist about schemas, columnar data, and offering opportunities for MFP pushdown is broader than any one 
problem. There may be alternatives to specific use cases that it opens up (e.g. vs `TRANSFORM USING` in the case of 
enabling append-only workloads, were we to pursue that problem), but there is not a clear alternative to the total 
feature set it helps enable.

# Unresolved questions
[unresolved-questions]: #unresolved-questions

<!--
- What questions need to be resolved to finalize the design?
- What questions will need to be resolved during the implementation of the design?
- What questions does this design raise that we should address separately?
-->

* The specific interface Persist can give to Compute to apply MFPs to part stats
* What format part statistics are stored in (Parquet with the same schema as the data? Proto? Something custom?), as well
where they are stored (Consensus vs Blob)

# Future work
[future-work]: #future-work

<!--
Describe what work should follow from this design, which new aspects it enables, and how it might affect individual parts of Materialize.
Think in larger terms.
This section can also serve as a place to dump ideas that are related but not part of the design.

If you can't think of any, please note this down.
-->

As noted in [Motivation](#motivation) there are many use cases that can fall out of this work, some more speculative
like support for schema evolution. There are also large classes of read performance improvements this work would 
enable (e.g. clustering data based on user-defined column order for prefix locality, compressing individual columns,
projection pushdown into our Blob storage reads, more statistics like HLL cardinality estimates or bloom filters
over values, etc)
