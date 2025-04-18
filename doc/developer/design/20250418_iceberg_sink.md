# Iceberg Sink

- Associated: *TBD*


## The Problem

Customers would like to export data from Materialize to external Iceberg tables for later analysis. The current solution for such customers involves an intermediate message broker, e.g. Kafka. This adds extra complexity and latency, where it could be avoided.

Customer preference is to have Materialize compact the data by a key (i.e. latest data for a given key). It is acceptable, at least to start, to have the analytics engine do the compactions.

As with all icebergs, most of it is below the surface.  Iceberg is a table specification, not a database engine.  It is designed for storing large amounts of data for analytics purposes. Iceberg is composed of 2 main componens; the catalog and an object store.  An engine (Spark, Flink, etc.) is responsible for implementing the specification to provide users the ability to modify data, as well as performing maintenance tasks on the data stored.

**References**
- https://iceberg.apache.org/docs/latest/
- REST Catalogs
    - [Apache Polaris](https://polaris.apache.org/)
    - [Snowflake Open Catalog (managed Polaris)](https://other-docs.snowflake.com/en/opencatalog/overview)
    - [Unity OSS Catalog](https://www.unitycatalog.io/)


## Success Criteria

- Ability to create an append-only iceberg sink in Materialize 
- Support for Icebery catalogs (REST, Hive, Glue, etc.)
- Support for S3 as a object store
- Support for S3Tables
- Support for Parquet format
- Single table per sink
- Exactly once delivery
- V2 Iceberg table format



Eventually, depending on need:
- Auth: oauth, auth bearer tokens
- Support for [identifier fields](https://iceberg.apache.org/spec/#identifier-field-ids) (TODO: not sure if we need this yet)


## Out of Scope

- Copy-on-write compaction
    - This requires rewriting tables when updates are made. Due to the heavy impact on writes, it is not appropriate for streaming use cases.
- V1 Iceberg table format
- File Formats beyond Parquet (ORC, Avro)
- [Positional Deletes](https://iceberg.apache.org/spec/#position-delete-files)
- Append Only (Delete and Update do not delete the old rows)?


## Solution Proposal

What do you sink?


### Materialize SQL


==TODO==
- review the S3Tables REST API in AWS to see if it matches the REST API spec for Iceberg



Add a new connection type for `ICEBERG CATALOG`.  This will provide a union of options for the various catalog types. An alternative would be to create typed catalogs, e.g. `CREATE CONNECTION foo TO ICEBERG REST CATALOG`, in which case the options are specifc to the type of catalog.

For S3Tables, the catalog is accessed via [AWS S3Table APIs](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Types_Amazon_S3_Tables.html), so it would use an `AWS` connection instead of a user / secret.   Note: `REGION` is a property of `AWS` connection, and signing will be done using that region, or `us-east-1` if no region is specified.  This is important as the signing has to match the region the service is being hosted from.

S3Tables APIs require passing the [S3 table ARN in requests](https://docs.aws.amazon.com/AmazonS3/latest/API/API_s3TableBuckets_CreateTable.html). We should probably configure the object store in the iceberg catalog, which doesn't make much sense.  Maybe we shouldn't call this the catalog, just ICEBERG?

```SQL
CREATE CONNECTION iceberg_catalog_connection TO ICEBERG CATALOG (
    TYPE = 'rest' | 's3table', -- determines which fields are required, optionally, we can replace CATALOG with REST|S3TABLE
    URI = 'https://my.awesome.cat:8081/api/catalog', -- REST
    USER  = 'materialize', -- REST
    PASSWORD SECRET = iceberg_pass, -- REST
    AWS CONNECTION = "...", -- S3Table / Glue?
    WAREHOUSE = '...', -- Optional: the root path of the data warehouse in the object store, we can provide a default
)
```

Create a connection for S3:
```SQL
CREATE CONNECTION iceberg_s3_conn TO AWS (
    ACCESS KEY ID = "ABC123",
    SECRET ACCESS KEY = "....",
    REGION = "eu-west-1",
)
```

https://docs.rs/iceberg-rest-catalog/latest/iceberg_rest_catalog/apis/configuration/struct.Configuration.html

Creating the SINK
```SQL
CREATE SINK iceberg_sink
FROM my_favorite_materialized_view
TO ICEBERG CATALOG iceberg_catalog_connection 
OBJECT STORE 's3://mybucket' USING AWS CONNECTION iceberg_s3_conn
-- FOR S3Tables, provide the ARN: OBJECT STORE 'arn:...:s3tables:...:bucket:/mybucket' USING AWS CONNECTION iceberg_s3_conn
WITH (
    DATABASE NAME = 'db1', -- Optional: part of the object store key, we can provide a default
    TABLE NAME = 'tbl1', -- Optional: part of object store key, we can provide a default
    -- IDENTIFIER FIELDS = ['id', 'timestamp'], -- Optional: identifier field ids TODO: not sure if we need this yet
);
```

==TODO==
Do we want to provide any controls for writing data out to the sink? For example, allow setting min size and/or min time.
The sink will write to iceberg only if min size or min time has been exceeded.  This would provide a bound on the size to prevent really small files, or provide a bound on how long before we push an update in case the updates are tiny.  Technically, we would always have a min size of 1 to avoid writing out an empty row. Note, this does not affect the upper bound for size. If there are many updates within a single transaction, we can write them into a single file (or multiple in parallel if we know it will be large - future enhancement).



### Transactional Writes

Iceberg table writes provide serializable isolation using snapshots and atomic swaps of metadata files for sigle tables.  Writes are performed optimistically, and may fail on the commit if the snapshot on which the write is based is no longer current.  It is up to the writer to retry the updated based on a newer snapshot.


### Single Writer



For a single replica with multiple workers, we can ensure only a single writer is managing the transaction by using `GlobalId & worker_id == 0` (as is done in Kafka sink).  There is the possibility for multiple replicas to be online concurrently (however briefly) for the same sink in Materialize, in which case it's possible that there are multiple writers.  For the Kafka sink, other writers are fenced using the [init_transactions](https://docs.rs/rdkafka/latest/rdkafka/producer/trait.Producer.html#tymethod.init_transactions) operation.


==TODO== 
- Look into what is available within MZ for this.  Could we lean on the same mechansim used by envd for fencing (could be noisy/costly)?

### Exactly Once Delivery

Kafka sinks create a dedicated progress topic to track what as been delivered to the topic(s) associated with sink (TODO: this might be connection). Publishing to the data and progress topic are done within a transaction.  Iceberg transactions are single table only, so we cannot create a secondary table and update it within the same transaction. A Materialize specific column will be added to the output table (`_mz_progress`) of type `long`.

==TODO==
- `long` is signed in Parquet, this might wrap if our progress is u64 (need to check) - but no one should care except for us?
- is there a way to quickly determine the last time we wrote to find the most recent progress?  If not, we may need to walk our queries backwards with growing windows (1s, 2s, 8s, 16s, ...).


### Deletes and Updates
An update in iceberg is a delete followed by a write.  Materialize will delete rows using [equality Deletes](https://iceberg.apache.org/spec/#equality-delete-files), which will always exclude the `_mz_progress` column (see above).  An update is a delete of an existing row followed by an insert of new data, within the same transaction.



### Parquet Output Options

==TODO==
- Rows groups per file?
- Dictionary encoding?
- Compression?


### Iceberg Table Maintenance

Iceberg, being a table specification, provides [guidance on iceberg table maintenance](https://iceberg.apache.org/docs/latest/maintenance/) tasks.  It is up to the query engine (Spark, Flink, etc.) to provide the implementation. S3Tables provides some of this functionality, where using a REST catalog would not.

**Snapshot Management**

Iceberg operates under snapshot isolation and snapshots are tracked via the catalog. Snapshots accumulate until they are expired, which a handled by the engine. Once a snapshot expires, any files in the object store that are no longer referenced by any snapshot are eligible to be deleted.  This is the responsibility of the engine maintaining the Iceberg table(s).

- [S3Tables provides snapshot management.](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-maintenance.html#s3-tables-maintenance-snapshot)
- [Snowflake Open Catalog does not write to the catalog, so does not provide snapshot management.](https://other-docs.snowflake.com/en/opencatalog/overview#key-concepts)

**Compaction**

Streaming for Iceberg involves writing a large number of small files to object storage and updating the catalog. The analytics engine must perform Merge-On-Read (MOR) to view the latest state.  Over time, the sparse nature of the data results in poor read performance. Compaction is the process by which many small files are rewritten as larger files to. Row level deletes are also applied at this time.

- [S3Tables provides compaction.](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-maintenance.html#s3-tables-maintenance-compaction)
- [Snowflake Open Catalog does not write to the catalog, so does not provide compaction.](https://other-docs.snowflake.com/en/opencatalog/overview#key-concepts)

**Cleanup of old/orphaned files**

Each append to a table will generate a new metadata file.  For streaming use cases, these will accumulate quickly as appends will likely be small.
One of the recommended tasks is to [remove old metadata files](https://iceberg.apache.org/docs/latest/maintenance/#remove-old-metadata-files`) - especially important for streaming as there are many small writes.  


In the event of a crash while writing to iceberg, it's possible that a replica leaves [orphaned files, which will need to be cleaned up](https://iceberg.apache.org/docs/latest/maintenance/#delete-orphan-files).

==TODO==
- it looks like there should be an option to prune metadata files on each write, not sure if it's available for the rust client, or what the overhead is.
- does S3Tables clean up orphaned files?


<!--
What is your preferred solution, and why have you chosen it over the
alternatives? Start this section with a brief, high-level summary.

This is your opportunity to clearly communicate your chosen design. For any
design document, the appropriate level of technical details depends both on
the target reviewers and the nature of the design that is being proposed.
A good rule of thumb is that you should strive for the minimum level of
detail that fully communicates the proposal to your reviewers. If you're
unsure, reach out to your manager for help.

Remember to document any dependencies that may need to break or change as a
result of this work.
-->

## Minimal Viable Prototype

<!--
Build and share the minimal viable version of your project to validate the
design, value, and user experience. Depending on the project, your prototype
might look like:

- A Figma wireframe, or fuller prototype
- SQL syntax that isn't actually attached to anything on the backend
- A hacky but working live demo of a solution running on your laptop or in a
  staging environment

The best prototypes will be validated by Materialize team members as well
as prospects and customers. If you want help getting your prototype in front
of external folks, reach out to the Product team in #product.

This step is crucial for de-risking the design as early as possible and a
prototype is required in most cases. In _some_ cases it can be beneficial to
get eyes on the initial proposal without a prototype. If you think that
there is a good reason for skipping or delaying the prototype, please
explicitly mention it in this section and provide details on why you'd
like to skip or delay it.
-->

## Alternatives

<!--
What other solutions were considered, and why weren't they chosen?

This is your chance to demonstrate that you've fully discovered the problem.
Alternative solutions can come from many places, like: you or your Materialize
team members, our customers, our prospects, academic research, prior art, or
competitive research. One of our company values is to "do the reading" and
to "write things down." This is your opportunity to demonstrate both!
-->

## Open questions

<!--
What is left unaddressed by this design document that needs to be
closed out?

When a design document is authored and shared, there might still be
open questions that need to be explored. Through the design document
process, you are responsible for getting answers to these open
questions. All open questions should be answered by the time a design
document is merged.
-->
