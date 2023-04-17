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

![mfp-temporal-filtering](https://user-images.githubusercontent.com/785446/224154154-2276a7d1-4149-407f-8737-9acd4e929396.png)

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

Thanks to our part statistics, we know the range of possible values any particular column may have.
To decide whether we can filter a part or not, though, we need to know the range of possible values of _the output of the MFP's filter:_ if we can prove that the filter will never return `true` or an error value, it's safe to skip the entire part.

Using the range of possible input values to compute the range of possible output values is a well-known static analysis problem, and it's commonly solved via [abstract interpretation](https://en.wikipedia.org/wiki/Abstract_interpretation). In practice, this involves:
- Choosing an "abstract set" to approximate your data type. For part filters, we can use roughly:
    ```rust
    /// Approximating a range of possible non-null datums.
    enum Values<'a> {
      Empty, // No non-null datums.
      Within(Datum<'a>, Datum<'a>), // An inclusive range.
      All, // Includes all datums.
    }
    
    /// Approximating a set of possible `Result<Datum, Err>`;
    /// ie. the result of `MirScalarExpr::eval`.
    pub struct ResultSpec<'a> {
      nullable: bool, // Could this result be null?
      fallible: bool, // Could this result be an error?
      values: Values<'a>, // What range of non-null values may be included?
    }
    ```
    This is meant to be compact enough to compute efficiently, but still precise enough to produce interesting results for many expressions.
- Writing an "interpreter" - for every possible expression, we need to be able to evaluate that expression as an instance of our `ResultSpec`. (As compared to the normal concrete interpreter, where the expression is evaluated as an ordinary `Result`.)
  - `Literal`s are interpreted directly: eg. `Datum::Null` becomes `ResultSpec { nullable: true, ..}`.
  - For `Column`s, we know the set of possible values from the statistics. For `UnmaterializableFunc`s like `mz_now()`, we can also provide a range of possible values up front.
  - The various `Func`s are the tricky case: given an arbitrary function, there's no way to compute the `ResultSpec` of possible outputs from the `ResultSpec`s of the inputs. However, if we know the function is _monotonic_, then we can map the input range to an output range just by mapping the endpoints! Conveniently, nearly all the functions that are relevant to temporal filters are monotonic in at least one argument: arithmetic, casts, `date_trunc`, etc. We propose annotating these functions with a `monotonic` annotation so they can be correctly handled by our interpreter. (To be conservative, we assume un-annotated functions might return any value, including nulls or errors.)

To evaluate an MFP for a particular part in the persist source, we:
- Calculate the `ResultSpec` for each column using our stats.
- Evaluate the MFP using the machinery above, returning a `ResultSpec` that captures the possible outputs of the filter.
- If the filter might return `true` or an error, we keep the part; otherwise, we filter it out.

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
* For record fields, whether we compute mins/maxs for each individual subcolumn, or do a lexicographical sort (sort col 1 first, secondary sort on col 2, tertiary sort on col 3, ... etc) for the whole value. e.g. if our dataset is `[(5, 10), (0, 20)]` do we return `(0, 20)` as the min, or store a min per column where the first field is `0` and the second field is `10`? Spitballing: since this is similar to how we return both min columns and rows at the outermost level, maybe we want to mirror that behavior and do both
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
