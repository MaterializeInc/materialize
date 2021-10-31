---
title: "Glossary"
description: "Glossary of terms for Materialize"
menu: "main"
weight: 3
---
Useful Terms for understanding Materialize

### Apache Kafka

is an [event](#event) [streaming](#streaming-stream) system. KAFKA is a registered trademark of The Apache Software Foundation.

### Batch processing

here: a periodical processing of a collection of [events](#event).

### Daemon

here: a computer program or software running unattendedly without user interface

### Engine

here: a core software component or system.

### Event

here: a change in data.

### Query

in [SQL](#sql), a definition of criteria for data retrieval

### RDBMS
Relational Database Management System 
<!-- TODO: expand -->

### Source

the origin of the data being processed: can be static (e.g., a file) or dynamic
(e.g., a [stream](#streaming-stream)).

### SQL

(pronounced like ‘sequel’); the Structured Query Language, one of the most widely used computer languages to manage data
in [RDBMS/relational databases](#rdbms) or [streams](#streaming-stream) derived from such data.

### Streaming (Stream)

collecting [events](#streaming-stream) and continuously presenting them for processing or consumption (as a stream).

### View

in SQL, a pre-defined, reusable [query](#query). Queries can be executed against views and the [RDBMS](#rdbms) will then optimize the
resulting query by requesting only the necessary data. Nevertheless, because views only store queries, not data, each
time a query is run against a view.
