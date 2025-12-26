# Iceberg Sink

- Associated: *TBD*


## The Problem

Customers would like to export data from Materialize to external Iceberg tables for later analysis. The current solution for such customers involves an intermediate message broker, e.g. Kafka.

Customer preference is to have Materialize compact the data by a key (i.e. latest data for a given key). It is acceptable, at least to start, to have the analytics engine do the compactions on read (Merge-on-Read).

As with all icebergs, most of it is below the surface.  Iceberg is a table specification, not a database engine.  It is designed for storing large amounts of data for analytics purposes. Iceberg is composed of 2 main componens; the catalog and an object store.  An engine (Spark, Flink, etc.) is responsible for implementing the specification to provide users the ability to read and modify data, as well as performing maintenance tasks on the data stored.



**References**
- https://iceberg.apache.org/docs/latest/
- REST Catalogs
    - [Apache Polaris](https://polaris.apache.org/)
    - [Snowflake Open Catalog (managed Polaris)](https://other-docs.snowflake.com/en/opencatalog/overview)
    - [Unity OSS Catalog](https://www.unitycatalog.io/)
    - [S3 Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-open-source.html)


## Success Criteria

- Support REST (including S3Tables) catalog
- Support S3 object store
- Support Parquet file format
- Automatic namespace and table creation
- Support for primary key in Iceberg
- Support for partitioning
- Exactly once delivery
- Support for V2 Iceberg table format
- Authentication
    - Username/Password
    - AWS Access Key Id / Secret Key
    - AWS IAM
- Support for different write modes
    - Upsert mode (applies deletes, updates, and inserts to iceberge table)

## Out of Scope
- Copy-on-write compaction
    - This requires rewriting tables when updates are made. Due to the heavy impact on writes, it is not appropriate for streaming use cases.
- V1 Iceberg table format
    - Vendors I surveyed only support writing V2. Support for V1 would make more sense for read access of existing data lakes, which isn't this use case.
- Support for creating iceberg sink based on catalog object
    - Kafka sink also does not support this, and there doesn't appear to be a need for this functionality as of this writing.
- Support for sized-based commit criteria
    - E.g., commit an update once it reaches 1GB.
- Iceberg Table Maintenance
    - This is something that customers would like to see, so possibly a feature that follows iceberg sink. Detaila [below](#iceberg-table-maintenance).
- Additional catalog support
    - Glue, Hive, Hadoop, etc.
- Additional object store support
    - GCP, ABS, etc.
- Support additional file types
    - Avro, ORC
- Support file format options
    - E.g., row group sizes or compression algorithm in Parquet files.
- Support for Append-only write mode
    - In this mode, deletes are either a no-op or written it to the table as a tombstone.

## Solution Proposal

### SQL

Materialize will provide users SQL commands to configure a catalog and object store.  The catalog and object store can have different types, but for the first iteration of this, this design will focus on REST catalogs (including S3Tables) and S3 as the object store.

Create a connection for the iceberg catalog:
```SQL
CREATE CONNECTION iceberg_catalog_connection TO ICEBERG CATALOG (
    TYPE = 'rest' | 's3table', -- technically these are both REST, but the authentication schemes are different, and S3Tables utilize WAREHOUSE
    URI = 'https://my.awesome.cat:8081/api/catalog', -- REST
    USER  = 'materialize', -- REST
    PASSWORD SECRET = iceberg_pass, -- REST
    AWS CONNECTION = "...", -- S3Table
    WAREHOUSE = '...', --- S3Table, this would be the table ARN, otherwise this is not required
)
```
*This approach will provide a union of options for the various catalog types. An alternative would be to create typed catalogs, e.g. `CREATE CONNECTION foo TO ICEBERG REST CATALOG`, in which case the options are specifc to the type of catalog.*

*S3Tables is special case of REST.  The catalog would use an `AWS` connection instead of a user / secret, as [REST requests to AWS require request signing using AWS SigV4](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-open-source.html#tables-endpoint-auth). Note: `REGION` is a property of `AWS` connection, and signing will be done using that region, or `us-east-1` if no region is specified.  This is important as the signing has to match the region the service is being hosted from. Additionally, the WAREHOUSE, which isn't generally used for REST catalogs, must be set to the S3Table ARN.*



Create a connection for S3 (this utilizes Materialize's existing `AWS CONNECTION`):
```SQL
CREATE CONNECTION iceberg_s3_conn TO AWS (
    ACCESS KEY ID = "ABC123",
    SECRET ACCESS KEY = "....",
    REGION = "eu-west-1",
)
```


Creating the SINK
```SQL
CREATE SINK iceberg_sink
FROM my_favorite_materialized_view
TO ICEBERG CATALOG iceberg_catalog_connection
USING AWS CONNECTION iceberg_s3_conn
NAMESPACE "namespace_name" TABLE "table_name"
WITH (
    COMMIT INTERVAL = '20m', -- commit data to the iceberg table every 20 minutes of MZ time, optional.
);
```
*Appending data to iceberg requires uploading metadata and data files, and reading data requires accessing metadata to know which data files need to be retrieved. It is inefficient to write very small updates, and very resource intensive for readers to perform the reads when there are many small updates.  To give users control over the dimensions of the appended data, Materialize will allow users to optionally specify a commit interval for the sink. Materialize will commit an update to the iceberg table after the commit interval has elapsed.  The commit interval is given in MZ time, which is the timestamp assigned by Materialize to events, disctinct from wall clock time.*


### Iceberg Concepts

First some terms (borrowed directly from the [table spec](https://iceberg.apache.org/spec/#terms))
- Schema -- Names and types of fields in a table.
- Partition spec -- A definition of how partition values are derived from data fields.
- Snapshot -- The state of a table at some point in time, including the set of all data files.
- Manifest list -- A file that lists manifest files; one per snapshot.
- Manifest -- A file that lists data or delete files; a subset of a snapshot.
- Data file -- A file that contains rows of a table.
- Delete file -- A file that encodes rows of a table that are deleted by position or data values.

Iceberg tables rely on optimistic concurrency to ensure atomic updates.  The high level flow is fairly straightforward:
1. write data files / delete files
1. write metadata files
1. issue a commit to the catalog to update the table metadata with the newly added files

The commit performs a compare and swap operation to update the metadata of the table, ensuring that only a single writer is able to update the table.  More information about the concurrency model is available in the [Apache Iceberg Table Specification](https://iceberg.apache.org/spec/#overview).


### Coordinator

The coordinator is responsible for DDL (creating iceberg namespace, iceberg table, etc.) and managing append transactions. A timely worker will be responsible for the coordinator function.  In the MVP, the coordinator will be responsible for all operations.  In subsequent versions, specifically after partitioning is added, Materialize can investigate distributing work to multiple workers.


#### - Automatic Namespace and Table Creation

When starting, the sink will retrieve the namespace information.  If it doesn't exist, the namespace will be created.  The same will be performed for the table. If the table does exist, the sink will **not** attempt to verify the schema. If the schema is incompatible, the write will fail regardless. It is not recommended that users create or modify the iceberg table.


#### - Appending data

Appending data includes insert, update, and delete row operations. To perform deletes, Materialize will generate [equality delete files](https://iceberg.apache.org/spec/#equality-delete-files) that match the fields of the primary key.  Inserts are performed via data files.  Updates contain delete files and data files in the same commit.  Materialize will enforce a 512MB limit (which I borrowed from S3Tables) on the size of the parquet files. Appends that are larger than 512MB will be composed of multiple files.

Materialize will utilize `Fast Append`, which avoids rewrites of manifest files (see [here](https://iceberg.apache.org/spec/#snapshots)). As part of the write, Materialize will store information in the snapshot [Summary properties](https://iceberg.apache.org/spec/#optional-snapshot-summary-fields). The properies field is a `HashMap<String, String>`, in which Materialize will store the timestamp and sink version number. To determine the latest append performed by Materialize, retrieve the most recent snapshots and examine properties. It is possible that snapshot expiration has cleaned up all Iceberg Table snapshots that contain Materialize information. In this case, the sink will not be able to determine the resume upper and will need to be recreated.  A conservative snapshot expiration policy would allow keeping a 5-7 days worth to avoid having to recreate the sink.

Appends to the iceberg table are performed transactionally. Data is written to one or more parquet files in the object store.  Upon completion of the writes, the append is committed to the iceberg table. Each append will reflect all updates from the last committed frontier to some later frontier. In upsert mode, every Iceberg snapshot will reflect the full contents of the upstream collection as-of some frontier.

The frequency of commits is determined by the commit interval, which is a duration of MZ time that must elapse since the last append (or start of the dataflow) before the sink attempts to commit changes to the iceberg table. If the commit interval is not set, the sink will perform an append for each event time.

The append process does not attempt to delete files on a failed commit. S3Tables, for example, does not allow `DeleteObject` and returns a 403 (in my testing, but I could not find this documented). Orphaned files will be cleaned up by [iceberg maintenance tasks](#cleanup-of-orphaned-files).

_**NOTE** For this functionality, Materialize will fork [iceberg-rust](https://github.com/apache/iceberg-rust).  The existing API in the crate is still evolving and some uses haven't been account for.  For example, it does not expose, or allow setting, the `SUMMARY` properties._

**More than one sink**

There may arise cases where more than one instance of sink exists.  A writer will store, in memory, the last successful commit performed to the iceberg table (either the snapshot_id, or the timestamp + version). This can be null when the sink is first created. On startup, a writer must retrieve the latest snapshot Materialize committed to the iceberg table (identified by the presence of the Materialize keys in the snapshot summary). In the event of a commit failure, the writer validates its last known commit is the last Materialize commit for the iceberg table (where the snapshot summary contains Materialize properties), or if there was no previous Materialize commit, there still isn't.

This approach likely will not allow a new replica to immediately take over, as fencing out an existing replica requires successfully appending data to the iceberg table. In the case where there is an existing replica and a new replica comes online, the existing replica will already be in the process of collecting updates into parquet files to append to the iceberg table. Likewise, the new replica may start with write conditions that allow for larger appends, meaning there will be a longer delay in performing them. Hence, this approach relies heavily on the orchestration to stop the existing replica before the new replica can make progress.


#### - Partitioning
The iceberg sink should eventually support the ability to utilize [partitioning](https://iceberg.apache.org/spec/#partitioning). A user can specify one or more columns, including transforms defined in the spec, to be used as the partitiong spec when the sink appends data to the iceberg table. A customer will be able to provide column names in addition to the transforms supported by iceberg partitioning.  The information will be stored in the `SinkDesc` for iceberg.

The iceberg create provides implementation of the [transforms](https://docs.rs/iceberg/latest/iceberg/spec/enum.Transform.html). Materialize will need to implement parsing for the `PARTITION BY` in Materialize SQL to ensure customer provides valid functions.  It would be ideal for the column names to be validated against the schema during purification, but in the case it isn't possible, it will be up to the customer to ensure the columns are correct.

When writing partitioned data, Materialize will partition the data into separate data files that match the specification.  The operation to perform the append is otherwise the same (with multiple data files instead of 1).

### Metrics

- iceberg write statistics
    - size (bytes)
    - duration
    - success
    - failure (preferrably by type)
- fencing metrics
    - fenced count (incremented when replica thinks it was fenced)

## Minimal Viable Prototype

A MVP implementation will support the following:
- New SQL
- REST catalog (including S3Tables)
- S3 for object store
- Authentication
    - Username/Password
    - Access Key/Secret Key
- Support for parquet file format with a fixed set of parquet writer options
- Support for upsert mode (this requires support for primary keys)
- No partitioning

## Alternatives
- Instead of utilizing the iceberg create to interact with iceberg, use the datafusion create to interact with SQL
    - If the sink output just SQL commands for DELETE and INSERT, then there is the potential for reuse with other SQL DBs.
    - For large updates, like the initial snapshot, the data could be loaded via DataFusion's EXTERNAL TABLE (requires writing the data as CSV/parquet somewhere and letting DF consume it.)
- Store Materialize properties (timestamp, sink version) within iceberg metadata
    - Table `properties` field
        - The [iceberg specification](https://iceberg.apache.org/spec/#table-metadata-fields) calls out that this field intended to control table reading/writing, not for arbitrary metadata.
- Alternatives to MZ time for `COMMIT INTERVAL`
    - Use wall clock time instead of MZ time
        - Doesn't require extra explanation, wall clock time is well understood, but MZ time is not.
        - Wall clock time requires extra handling to ensure that if the commit interval has expired, all updates at the same event time are still part of the same iceberg transaction.
- Using metadata DB and a 2PC approach for fencing and tracking write transactions (a very abridged version)
    - This approach requires maintianing a record in the form of `(GlobalId, epoch, phase, payload)`. A coordinator will condtionally upsert a new record when initiating a transaction, `(GlobalId, epoch+1, phase, payload)` for `(GlobalId, epoch)`.  If this fails to update due to a contraint or condition violation, the process(es) that failed are fenced. Phase would be either `precommit` or `commit`.  Payload, for a sink, would be `(ts, sink_version, snapshot_id)` - denoting the greatest MZ timestamp contained within the apped, the version of the sink, and the snapshot_id intended to be stored there.
    - A writer will record a message with `epoch+1` and phase `precommit`.  Start a transaction to update the record to `epoch+1` and phase`commit`, commit to iceberg, finally commit the transaction in the metadata store.
    - If, on startup, the state is `precommit`, the recovery process would be to validate the snapshot was applied.  Update phase to `commit`, otherwise, retry it.
    - This assume that the metadatadb is not using optimistic concurrency, and a writer that is in the middle of a transaction
        - blocks others trying to perform the update
        - will drop the transaction if it disconnects.
    - An external service would have to clean these up based on the catalog (`GlobalIds` that don't exist don't need to be in this table). A sink that's shutting down is unlikely to be able to clean this up.

## Open questions

- TBD

## Extra Bits and Bobs

### Iceberg Table Maintenance

Iceberg, being a table specification, provides [guidance on iceberg table maintenance](https://iceberg.apache.org/docs/latest/maintenance/) tasks.  It is up to the engine (Spark, Flink, etc.) to provide the implementation. S3Tables provides this functionality, where using a REST catalog would not. Implementing this functionality in Materialize would require a long running, asynchronous service.  This could run on a replica, but will also require some method of fencing to ensure that multiple instances of this service are not actively trying to perform maintenance tasks on the same table.  Because this service will need to read, compact, and write back data to the iceberg table, it will compete with dataflows in the same replica, making capacity planning more complex and affecting the performance characteristics of the sink.

#### Snapshot Management

Iceberg operates under snapshot isolation and snapshots are tracked via the catalog. Snapshots accumulate until they are expired, which a handled by the engine. Once a snapshot expires, any files in the object store that are no longer referenced by any snapshot are eligible to be deleted.  This is the responsibility of the engine maintaining the Iceberg table(s).

- [S3Tables provides snapshot management.](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-maintenance.html#s3-tables-maintenance-snapshot)
- [Snowflake Open Catalog does not write to the catalog, so does not provide snapshot management.](https://other-docs.snowflake.com/en/opencatalog/overview#key-concepts)

#### Compaction

Streaming for Iceberg involves writing a large number of small files to object storage and updating the catalog. The analytics engine must perform Merge-On-Read (MOR) to view the latest state.  Over time, the sparse nature of the data results in poor read performance. Compaction is the process by which many small files are rewritten as larger files to. Row level deletes are also applied at this time.

- [S3Tables provides compaction.](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-maintenance.html#s3-tables-maintenance-compaction)
- [Snowflake Open Catalog does not write to the catalog, so does not provide compaction.](https://other-docs.snowflake.com/en/opencatalog/overview#key-concepts)

#### Cleanup of orphaned files

Each append to a table will generate a new metadata file.  For streaming use cases, these will accumulate quickly as appends will likely be small.
One of the recommended tasks is to [remove old metadata files](https://iceberg.apache.org/docs/latest/maintenance/#remove-old-metadata-files`) - especially important for streaming as there are many small writes.

In the event of a crash while writing to iceberg, it's possible that a replica leaves [orphaned files, which will need to be cleaned up](https://iceberg.apache.org/docs/latest/maintenance/#delete-orphan-files).  S3Tables does perform unreferenced file removal, see [AWS documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-considerations.html#s3-tables-unreferenced-file-removal-considerations) for additional details.
