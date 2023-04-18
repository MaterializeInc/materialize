- Feature name: Persist Filter Pushdown
- Associated: https://github.com/MaterializeInc/materialize/issues/12684

# Summary
[summary]: #summary

TODO

# Motivation
[motivation]: #motivation

Blobs written by Persist today are a black box: we have no way to characterize their contents without pulling down every part from Blob storage and decoding every row they contain.
This requires us to fetch every part no matter what question is being asked of the data.

By introducing types and capturing statistic metadata on parts, Persist can
more selectively filter which parts are fed into a dataflow, before even reading them from Blob storage.
When reading data with the appropriate locality, MFP pushdown will enable faster dataflow rehydration and faster ad-hoc / exploratory queries in ways that are not currently achievable.

# Explanation
[explanation]: #explanation

## Schemas

A `Schema` trait will be introduced to map data back and forth between Persist columns. Any Rust type
that is to roundtrip through Persist must have a corresponding `Schema`.

TODO: description of Row's Schema implementation.

## Part Statistics

When Persist writes a batch to Blob storage, it will compute basic statistics over its columns, when possible,
and write this data alongside existing metadata in Consensus. To start, we will collect a column's
non-null min and max values and null count, as well as the min/max Row within the part.

Statistics are optional: Persist may not be able to compute min/max values for all types, and we may need to drop statistics to keep metadata from getting too big.

## Filtering

Our compute layer pushes an `MapFilterProject` operator into the Persist source, which can filter and transform data before emitting it downstream.
Before each batch part is fetched from Blob storage in `persist_source`, the source will use our new part statistics to analyze whether that MFP will filter out _all_ the rows in that part.
If so, we get to skip fetching the part from Blob storage entirely.

This requires that we have some way to figure out what an MFP's `MirScalarExpr`s might return given only basic information like the nullability and range of values. For more on how that can be done, see the section on [filter pushdown].

Not all parts may have statistics, and those statistics may not be complete.
When stats are not present, this analysis needs to be conservative: if we don't have statistics for a column, we'll assume it might contain any legal value.

## Filtering & Locality

Having column statistics is a necessary building block to filter out parts before fetching them, but it is not guaranteed to be effective in actual queries unless there is some degree of locality to the filtered
column values across parts in Blob storage.
(If the data that matches our filter is evenly distributed across parts, we'll end up needing to fetch every part no matter how clever our analysis is!)

In our first implementation, we plan to focus on _temporal filters_: ie. filters on columns that are correlated with wall-clock time.
These filters tend to have very good locality, thanks to how Persist ingests and compacts data over time --
the newest, and
typically smallest, parts will contain the most recent updates, allowing one to filter out the older/larger
parts and their data effectively. This is particularly effective for fetching recent data from append-only sources;
we'll often have accumulated considerable numbers of older, larger parts that are no longer relevant to the filter.

![mfp-temporal-filtering](https://user-images.githubusercontent.com/785446/224154154-2276a7d1-4149-407f-8737-9acd4e929396.png)

# Reference explanation
[reference-explanation]: #reference-explanation

## Calculating statistics

TODO

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

## JSON columns

TODO - justify, describe both halves of the implementation

# Rollout

## Testing and observability
[testing-and-observability]: #testing-and-observability

Because Persist is so core to Materialize, we get a large amount of test coverage from existing tests. We can additionally write:
* Proptests to verify that all column types that we support statistics on sort equivalently to their Datum counterparts.
* Benchmarks to verify the new format's performance in reads and writes.
* Tests to verify that the new expression interpreter is consistent with `eval`.

Persist already has considerable metrics coverage; we should be able to verify the latency and error rate impacts of the feature from our existing metrics.
We'll also want to track the positive impacts of the feature, reporting metrics on the number of parts or bytes that are filtered out.

## Lifecycle
[lifecycle]: #lifecycle

We plan to roll out MFP pushdown behind two feature flags.
* Stats collection: when the collection flag is enabled, stats will be calculated and stored along with the Persist metadata for each part.
* Stats filtering: when the filtering flag is enabled, we'll use any statistics that are present to try and filter out parts in the Persist source.

If the filtering code is incorrect, we risk dropping parts that would have contributed rows to the query, returning incorrect results. Collecting stats also has a performance impact, increasing both the CPU cost of writing to Persist and the size of the resulting metadata.
We'll want to ramp these flags carefully, rolling out to individual test accounts and staging long before writing data in prod.

Neither of these flags should have user-visible effects, aside from the performance improvement itself.
It should be safe to roll back any breakage just by disabling the flag.

# Drawbacks
[drawbacks]: #drawbacks

Collecting statistics and interpreting MFP expressions both require a significant amount of code, and errors could result in incorrect output.
We should validate that the real-world latency improvements our users will see are large enough to justify the time investment and the added risk.

# Conclusion and alternatives
[conclusion-and-alternatives]: #conclusion-and-alternatives

`TRANSFORM USING` has been discussed as another way to enable append-only workloads.
While some use-cases can be addressed with either feature, filter pushdown has the advantage of not altering how the data is written.

We've explored various approaches to extracting part filters from the MFP data, including matching on the top-level structure of the filter. However, the simpler approaches aren't able to cope with many real-world temporal filters.

TODO

# Unresolved questions
[unresolved-questions]: #unresolved-questions

TODO

# Future work
[future-work]: #future-work

## Better support for non-temporal filters

Filtering on a prefix of the `(key, value)` columns. This is due to how parts within Persist are sorted
by `(key, value, ts)` in column/lexicographic ordering. In practice, the default column order is unlikely to
  be immediately useful for most collections, and column ordering would likely need to be user-specifiable
  to take full advantage of this property.

## Columnar on-disk format

### Projection pushdown

TODO

### Logical Sharding

By enabling faster / more selective reads, we additionally open up higher-level ideas like logical sharding,
where we store the data of many collections in a single physical shard. An example of this could be our existing
system tables -- rather than dedicating a Persist shard to each one, we could use a single physical
shard and rely on MFP pushdown to reduce reads to (mostly) only fetch data for individual system tables.
Doing so could vastly reduce the cost of small shards, particularly in their load on CRDB and PUT cost to S3,
with minimal impact on read performance.

### Schema evaluation

Understanding schemas at the persistent data level gives us a building block [to explore schema evolution at
the Persist level](https://github.com/MaterializeInc/materialize/issues/16625).

### Internal improvements

Less tangible motivation for this work is that by columnarizing and summarizing parts, we will have a forcing
function to progress some lesser-seen concerns within Persist. Examples of this include how proper columnar
storage will reduce allocations and memory pressure caused by Persist (something we're already grappling with),
and how storing statistics will require a more long-term scalable solution to how we store metadata. These are
problems we know we must stay on top of, and working through the building blocks for MFP pushdown will help frame
a long-term vision to solving them.
