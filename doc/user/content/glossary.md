---
title: "Glossary"
description: "Glossary of terms for Materialize"
menu: "main"
weight: 3
---
Useful terms for understanding Materialize.

### Apache Kafka

is an [event](#event) [streaming](#streaming-stream) system. (See [their website](http://kafka.apache.org/).) 
<!-- KAFKA is a registered trademark of The Apache Software
Foundation. -->

### API

<!--- TODO -->

### Arrangement

<!--- TODO -->

### Batch Processing

here: a periodical processing of a collection of [events](#event). See also [ETL](#etl).

### Casting

<!--- TODO (types of data) -->

### Catalog

<!--- TODO -->

### CDC

<!--- TODO -->

### Collection

<!--- TODO -->

### Confluent

<!--- TODO -->

### Contention

<!--- TODO -->

### Coordinator

<!--- TODO -->

### CTE

<!--- TODO -->

### Dataflow

<!--- TODO -->

### Daemon

here: a computer program or software running unattendedly without user interface

### Debezium

a software for [change data capture](#cdc), built on top of
the [Apache Kafka](#apache-kafka) [stream](#streaming-stream) processor.
(See [their website](https://debezium.io/).)

### Differential

<!--- TODO -->

### Engine

here: a core software component or system.

### ETL

<!--- TODO  ETL stands for “extract, transform, load”, and that’s exactly what’s going on. -->

### Event

here: a change in data.

### Functional Programming

<!--- TODO -->

### Index

<!--- TODO -->

### Join

<!--- TODO -->

### Kinesis

<!--- TODO -->

### Key

<!--- TODO -->

### LSM

<!--- TODO -->

### Materialize

<!--- TODO -->

### materialized ([daemon](#daemon))

<!--- TODO -->

### Materialized View

<!--- TODO -->

### Multiset

<!--- TODO -->

### Package

<!--- TODO -->

### OLAP

<!--- TODO -->

### OLTP

<!--- TODO -->

### OLVM

<!--- TODO (online view maintenance https://materialize.com/blog-cmudb/) -->

### Partition

<!--- TODO -->

### Persistence

<!--- TODO -->

### Plan

<!--- TODO (dataflow) -->

### PostgreSQL

<!--- TODO -->

### Query

in [SQL](#sql), a definition of criteria for data retrieval

### RDBMS

Relational Database Management System
<!-- TODO: expand -->

### S3

<!--- TODO -->

### Schema

<!--- TODO (Confluent) -->

### Seeding

<!--- TODO (union) -->

### Shell

<!--- TODO -->

### Source

the origin of the data being processed: can be static (e.g., a file) or dynamic
(e.g., a [stream](#streaming-stream)).

### SQL

(pronounced like ‘sequel’); the Structured Query Language, one of the most widely used computer languages to manage data
in [RDBMS/relational databases](#rdbms) or [streams](#streaming-stream) derived from such data.

### State

<!--- TODO -->

### Streaming (Stream)

collecting [events](#streaming-stream) and continuously presenting them for processing or consumption (as a stream).

### Timely dataflow

<!--- TODO -->

### View

in SQL, a pre-defined, reusable [query](#query). Queries can be run against views and the [RDBMS](#rdbms) will then
optimize the resulting query by requesting only the necessary data. Nevertheless, because views only store queries, not
data, each time a query is run against a view, the data retrieval has to be re-executed, in contrast
to [materialized views](#materialized-view).

### Volatility

<!--- TODO  (see also State) -->

### Worker

<!--- TODO -->

### Working Set

<!--- TODO -->


