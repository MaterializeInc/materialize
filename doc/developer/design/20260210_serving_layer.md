# Serving layer

## The Problem

Selects are slow.
In the best case, we can support around 5'000 queries per second because Materialize hasn't been optimized for fast selects, but rather for efficient incremental view maintenance.
We optimize each query, select its timestamp, send it to a replica, wait for compute to gather results, merge the results and send it back to the client.
Several steps along this path incur latency, and this design shows alternatives that have the potential to avoid it.
This design focuses exclusively on read transactions---the serving layer does not support writes---and outlines a system that should be able to handle 100s of 1000s requests per second.

## Success Criteria

Materialize can serve queries at a rate of 100k per second.
More is better, but not required.

## Out of Scope

The serving layer provides access to a defined subset of data.
The capabilities depend on the option chosen:
Option 1 would provide whatever API the downstream system offers.
Option 2 would be limited to key lookups and potentially range scans.
Option 3 would delegate to the application running on Materialize.

The actual API surface and specific SQL statements are out of scope for this design.
This document focuses on the architectural direction and trade-offs between options.

## Solution Proposal

To increase the queries per second, we need to change the architecture of Materialize, as incremental improvements will not allow us to reach orders-of-magnitude better performance.
The main bottlenecks are per-query work (optimization), serializing points in code (communication with clusters), and inefficient data structures for key lookups (arrangements).
We tackle all individually, although for some we have a menu of options at our disposal.
The following sections present three broad options, each with different trade-offs around scope, API commitment, and long-term flexibility.

At the moment, the environmentd process optimizes queries, handles communication with the client and cluster replicas.
Cluster replicas maintain indexes as arrangements, and handle peeks to read information from arrangements.
Clusters scale horizontally by adding more and bigger replicas, at the expense of causing higher tail latency.
Environmentd currently does not scale horizontally, but we could scale it as a whole (which is a separate project), or extract behavior from environmentd to handle it elsewhere, which is the main idea we follow in this design.

### Option 1: Build an excellent sink

Instead of building a serving layer inside Materialize, we build best-in-class sinks to external systems (Postgres, Redis, DynamoDB, etc.) and let users host their own serving layer.

**Advantages:**
 * Upholds transactional guarantees end-to-end.
 * Lets us focus on what we need to be good at: incremental view maintenance and data egress.
 * Users choose the serving technology that fits their latency, throughput, and operational requirements.

**Disadvantages:**
 * Reconciliation with the downstream system is hard in the presence of failures.
   A sink must be able to resume from a consistent point, and the downstream system must tolerate replayed or reordered writes.
 * Users bear the operational burden of the serving infrastructure.
 * We don't capture the value of serving---it remains outside the Materialize ecosystem.

### Option 2: Build a built-in serving layer

We build a serving layer as part of Materialize, optimized for the specific access patterns we can support well (key lookups, and potentially range scans).

**Advantages:**
 * Serves the immediate customer need for low-latency, high-throughput reads.
 * Tight integration with the timestamp oracle, persist, and the catalog.
 * Relatively contained scope if we limit the supported query shapes.

**Disadvantages:**
 * Risks locking us into API decisions that are hard to change later.
 * A first version will likely not be the best long-term design.
 * Expands the surface area of what Materialize must operate and support.

Details on this option follow in the sections below.

### Option 3: Build an application platform API

Rather than building a specific serving layer, we invest in an API that allows others to deploy applications on Materialize---similar to how Snowflake allows it with Native Apps and Snowpark Container Services (see [Prior art: Snowflake](#prior-art-snowflake) below).

We would design and expose APIs to interact with the catalog and persist, and build a reference serving layer *outside* the Materialize repository that demonstrates how to use them.

**Advantages:**
 * Fills a gap we have anyway: there is no programmatic API for third parties to build on Materialize.
 * Does not lock us into a specific serving layer design---the API is the product, and serving layers are applications.
 * Enables an ecosystem of applications beyond just serving (dashboards, caches, custom sinks, etc.).

**Disadvantages:**
 * Requires designing a stable API surface (REST? gRPC? Rust SDK? WASM SDK?), which is a significant commitment.
 * The reference serving layer still needs to be built and maintained.
 * Higher up-front investment before customers see value.

### Serving layer considerations

A serving layer (whether built-in or as a reference application) has to handle requests for data, at a system-determined time, and provide responses to clients at low latency and high throughput.
It must scale horizontally and be isolated from other serving layer replicas.

Choices:
 * Fast data structure to lookup values by key.
   We may support range scans, but not arbitrary inequality comparisons.
 * Reading from persist as the only way to get data into a serving layer.
   We can allow map/filter/projects (without temporal filters).
 * Isolation levels must be respected, i.e., we need to maintain multiple timestamped values per key, and their diff.
 * We can offer different interfaces.
   To handle complexity, we might not want to rely on SQL, but rather offer a REST API.
   All interfaces need to pre-define the queries the serving layer can handle.
   We can offer an implicit API (observe frequent queries), or an explicit API (declare queries).

### Interface considerations

The choice of client-facing interface has significant long-term consequences.

**pgwire (restricted SQL):**
Reuse the existing Postgres wire protocol, but restrict the supported query shapes (e.g., only prepared statements with equality-bound parameters, or only lookups on declared indexes).
Customer feedback favors this approach: pgwire is supported in every language, and abandoning it risks having to rebuild SQL features (transactions, session state, type system) into a new API later.
The main concern is whether pgwire's serialization and parsing overhead is compatible with high-throughput serving.
Advanced Postgres types (record, list, composite) lack standardized cross-language decoders, which is a pain point independent of the serving layer but relevant to API design.

**REST API:**
Simpler to build, naturally stateless, easy to load-balance.
Endpoints would map directly to declared indexes or prepared query shapes.
Loses the flexibility of SQL syntax and requires clients to adopt a new API.

**Declared prepared statements:**
Note that standard SQL prepared statements are scoped to a session, which makes them unsuitable for a shared serving layer.
We would need a new concept---a *declared query* or *named endpoint*---that is catalog-scoped and maps parameter bindings to index lookups.

### Prior art: Catalog-scoped parameterized queries

Most databases (PostgreSQL, MySQL, CockroachDB, SQL Server, Trino, DuckDB) scope prepared statements to a session.
A serving layer needs something longer-lived.
Three systems have catalog-scoped parameterized queries, and one has a proven cross-session caching model:

 * **ClickHouse parameterized views** are DDL objects with typed parameters, queried as table functions.
   They are persistent catalog objects, but each invocation is re-planned from scratch.
 * **Amazon Athena** supports persistent prepared statements scoped to a workgroup (not a session), with positional parameters.
   Like ClickHouse, each execution is re-planned.
 * **Hasura Native Queries** store parameterized SQL in Hasura's metadata (outside the database catalog) and expose them as GraphQL fields.
   Again, re-planned on each execution.
 * **Cassandra/ScyllaDB** takes a different approach: prepared statement IDs are content hashes of the query text, cached per-node.
   Any connection can execute by ID.
   This is operationally proven at scale but ephemeral---cached entries are evictable and not named catalog objects.

No existing system combines catalog persistence with pre-computed execution plans.
A Materialize serving layer that pre-computes the mapping from parameters to index lookups would be novel in this regard.

An alternative to explicit declaration is **implicit derivation from schema objects**, as PostgREST does: it introspects the database schema and generates REST endpoints for all tables and views.
The serving layer equivalent would be: if an index on `(customer_id)` exists, lookups by `customer_id` are automatically available without a separate declaration step.

### Option 2 details: Built-in serving layer replicas

We introduce the notion of a serving cluster with serving cluster replicas.
Its task is to handle specific queries, and return results at low latency.

#### Connection model

There are two main options for how clients reach serving replicas:

**Environmentd as a bump on the wire:**
Clients connect to environmentd, which forwards requests to the appropriate serving replica.
This is simple to implement---environmentd already terminates client connections and can route based on the query shape and key.
The downside is that environmentd becomes a bottleneck: all serving traffic flows through a single process, which defeats the purpose of a horizontally scalable serving layer.

**Balancerd as a routing proxy:**
We extend `balancerd` to route clients directly to serving replicas.
Currently, balancerd has no knowledge about clusters---it uses the SNI to forward connections to a specific environmentd and passes through the binary stream without inspecting it.
To route to serving replicas, balancerd would need to know which clusters exist and how to reach their replicas.
We would also need to decide to what extent balancerd terminates connections (parsing enough of the protocol to extract routing information) versus passing through the binary stream as it does today.
This approach avoids the environmentd bottleneck and provides better tenant isolation, but requires significant changes to balancerd's responsibilities.

**Option 3 considerations:**
For an application platform, external apps may need to accept direct connections from the outside rather than routing through Materialize's proxy.
Alternatively, we define a set of supported protocols (HTTP, pgwire, gRPC) and proxy only those.
The choice depends on how much we trust the app and how we integrate it into our cloud deployment---a fully trusted app could get direct network exposure, while an untrusted one would need to sit behind our proxy with protocol-level enforcement.

#### Routing and sharding

For key point lookups, we can take advantage of the existing cluster structure and data hashing.
Each serving replica (process) handles part of the key space, and a routing layer routes requests to the replica that holds the relevant data based on the key's hash.
This piggy-backs on the existing cluster replica infrastructure.

#### Error distribution

Errors taint access to data and must be distributed globally across all serving replicas.
However, the system can return any error that applies---it is not required to return a specific one.
This simplifies the error propagation mechanism: a replica that discovers an error broadcasts it, and any replica can surface it to a client requesting affected data.

#### Query support

Supporting SQL prepared statements (or an equivalent):
 * Investigate lateral joins.
 * If the placeholders are equality lookups, we can convert the query to a materialized view that arranges the data such that the placeholder columns are keys to the prepared statement.
 * An API endpoint is simpler, as it would define equality lookup on materialized views based on specific columns.

As we're not targeting arbitrary SQL queries, we do not need to touch the optimizer in surprising ways---the biggest change would be to optimize queries with placeholders into a predictable shape.

What we need:
 * Access to the timestamp oracle to determine appropriate timestamps for queries.
 * A parallel data structure mapping `(key, time) -> (value, diff)`.
   The diff would always be positive.

### Transaction isolation

Materialize by default provides strict serializable isolation, which guarantees the freshest results and no regressions in time for subsequent reads.
This comes at a cost: we need to establish what the freshest timestamp is by interacting with the timestamp oracle, and then wait until all objects in the query domain are up-to-date at that timestamp.
This means we potentially need to wait for the processing delay induced by Materialize's incremental view maintenance pipeline.
Some of the oracle interaction can be amortized (e.g., batching timestamp requests), but the fundamental constraint remains: strict serializability requires waiting for data to catch up to the chosen timestamp.

For a serving layer targeting high throughput and low latency, there is a tension:
 * **Strict serializable:** Preserves one of Materialize's core differentiators. Each read sees the freshest consistent state. However, reads may block waiting for data to arrive at the selected timestamp, which adds latency and limits throughput.
 * **Serializable (not strict):** We can serve reads at any consistent timestamp, including older ones where data is already available. This avoids waiting for the processing pipeline but loses the freshness guarantee---consecutive reads from the same client could see data go "backwards" in time.

It is unclear what isolation level a serving layer should provide.
Degrading to serializable loses one of Materialize's key values, but it is not obvious how to achieve high query throughput under strict serializability without significant amortization or relaxation of the freshness requirement.
The answer may depend on the use case: some applications need the freshness guarantee, while others would happily trade it for lower latency.

### Considerations for parallel data structures

Whatever data structure we select, we must generate it from a stream of updates, and must be able to read it in constant or logarithmic time.
Hot keys should be faster to access than cold ones.

We need an LSM-like structure that maps keys to values with `(time, diff)` annotations, supporting amortized updates at low tail latency.

* **LSM tree:** We maintain an LSM asynchronously in the background.
  Each layer maps keys to their full history within the time bounds of a read and write frontier.
  This is the most promising approach: writes are amortized through background compaction, and reads are logarithmic in the number of levels.
* **Hash maps:** Hard to make concurrent and would require atomic operations per key.
  Depending on the workload, it could suffer from contention, for example when recently modified keys are hot.
* **Van Emde Boas trees:** An option for externalizing data with efficient predecessor/successor queries, which could support range scans if we choose to offer them.
* **Per-query or per-connection caches:** We can layer caches on top of the primary structure to accelerate access to frequent hot keys.

## Prior art: Snowflake

Snowflake is the closest comparable platform in terms of a cloud-native analytical database with an application ecosystem.
Understanding their approach helps frame what we would and would not be building.
The following subsections cover their query serving model, SQL API, application framework, and container services, followed by a comparison to the options proposed above.

### Snowflake's query serving model

Snowflake has no dedicated low-latency serving layer.
All query paths---SQL API, ODBC/JDBC, Snowpark---go through warehouse-based execution with job-queue scheduling.
Their closest analog to a serving layer is *Interactive Tables* combined with *Interactive Warehouses*, which are optimized for fast, simple queries (selective `WHERE` clauses, optional `GROUP BY`) at high concurrency.
However, these still go through the warehouse execution path and do not bypass query optimization or scheduling overhead.

This means a Materialize serving layer with pre-materialized data in an optimized lookup structure, served directly without query optimization or warehouse scheduling, would be a differentiating capability.

### Snowflake SQL API

Snowflake exposes a REST API (`POST /api/v2/statements`) for executing SQL statements.
Queries that complete within 45 seconds return results synchronously; longer queries use async polling.
Results come back as JSON arrays, paginated for large result sets.
This is a general-purpose query API, not a serving API---it has the same latency characteristics as any other Snowflake query.

### Snowflake Native Apps Framework

Snowflake's Native Apps Framework uses a provider-consumer model: a provider creates an *application package* containing logic (stored procedures, UDFs, Streamlit apps), data shares, and a setup script.
Consumers install the app into their own account, where it runs as a database object with its own namespace.

Apps access provider data through secure data sharing (read-only) and consumer data through explicit privilege grants (`GRANT CALLER`).
Procedures execute with owner's rights by default, and restricted caller's rights are available for controlled access to consumer objects.

This model is relevant to Option 3: if we build an application platform API, we face similar questions about how applications access catalog objects, what privileges they run with, and how data is shared between the platform and the application.

### Snowpark Container Services

Snowpark Container Services (SPCS) is a managed container platform built into Snowflake.
Users push OCI-compliant Docker images, allocate compute pools, and deploy long-running services.
Containers access Snowflake data via an injected OAuth token and standard connectors.

Key properties:
 * Containers run on dedicated compute pools (not shared with other accounts).
 * Network is locked down by default---no egress, private endpoints only, public endpoints require explicit grants.
 * Service-to-service communication goes through declared endpoints with RBAC.

This is the most direct parallel to Option 3.
If we built an application platform, external applications (including a reference serving layer) would need:
 * Authenticated access to persist (analogous to Snowflake's injected OAuth token).
 * A way to discover and subscribe to catalog objects (analogous to Snowflake's data sharing).
 * Network isolation and access control (analogous to SPCS's locked-down networking).

### Snowflake Open Catalog (Polaris)

Snowflake Open Catalog exposes an Apache Iceberg REST Catalog API, allowing any compatible engine (Spark, Flink, Trino) to discover and read Iceberg tables in Snowflake-managed storage.
This is a catalog API, not a query serving API, but it demonstrates the pattern of exposing catalog metadata to external consumers---which is exactly what we would need for Option 3.

### Parallels and differences

| Dimension | Snowflake | Materialize (proposed) |
|---|---|---|
| Low-latency serving | Interactive Tables (still warehouse-based) | Dedicated serving layer with pre-materialized data |
| Query API | REST SQL API, ODBC/JDBC, Snowpark | pgwire (restricted), REST, or declared queries |
| App platform | Native Apps + SPCS | Option 3: catalog + persist API with reference apps |
| Catalog access for apps | Data sharing + privilege grants | To be designed (persist API + catalog subscriptions) |
| Data freshness | Warehouse must re-query | Continuous updates from persist |
| Compute isolation | Warehouse-level or compute pool-level | Cluster replica-level |

The key insight is that Snowflake has no equivalent to what we are proposing in Option 2.
Their app platform (relevant to Option 3) is mature but designed for batch and interactive analytics, not for sub-millisecond key-value serving.
A Materialize serving layer would occupy a distinct niche that no comparable platform currently fills.

## Alternatives

### Scale environmentd horizontally

We have plans for unrelated reasons to scale environmentd horizontally, and this could get us to a point where we can support faster queries.
We'll treat this as orthogonal for the purpose of this design as we're designing a separate serving layer under the control of environmentd.

## Open questions

* Which option do we pursue? Options 2 and 3 are not mutually exclusive---Option 3 could be the long-term direction with Option 2 as the first application built on the platform API.
* What's the right data structure? An LSM is the most promising candidate, but the details of time management and compaction need design.
* How do clients talk to the serving replica?
  Is environmentd a bump on the wire, is there a separate service, or do we allow direct connections?
  Customer feedback favors direct connections for tenant isolation.
* Do we support range scans, or only point lookups?
  Customer feedback indicates range scans (`WHERE X < ... ORDER BY X LIMIT N`) are important for paginated access patterns.
* What interface do we expose?
  pgwire with restrictions is the safest choice (avoids rebuilding SQL features), but we need to validate that pgwire parsing/serialization overhead is acceptable at target throughput.
* For Option 3: what is the API surface? REST, gRPC, Rust SDK, WASM SDK, or some combination?
* How does the serving layer interact with the catalog?
  Direct connections to a serving layer need a mechanism for syncing catalog changes and verifying strict serializability (cf. database-issues#9738).
* What isolation level does the serving layer provide?
  Strict serializable preserves Materialize's differentiator but requires waiting for data freshness.
  Serializable enables higher throughput but sacrifices freshness guarantees.
* What are the constraints on parameter usage in queries?
  Each parameter likely must appear in an equality constraint (or range constraint) with a column, and cannot be free-standing (e.g., `SELECT :param` is not a meaningful serving layer query).
