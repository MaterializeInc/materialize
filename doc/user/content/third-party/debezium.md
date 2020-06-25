---
title: "Using Debezium"
description: "Get details about using Materialize with Debezium"
menu:
  main:
    parent: 'third-party'
---

[Debezium](https://debezium.io/) works with Materialize in two capacities:

- Debezium adds change data capture (CDC) to MySQL or PostgreSQL databases,
  which lets you propagate data from your database to Materialize
- Debezium's CDC data (known as its envelope) is formatted in a way that
  expresses arbitrary operations (e.g. updates and deletes) on the database's
  data. This envelope is a powerful feature that lets Materialize perform more
  complex analysis by understanding how upstream data is changing.

For the best experience using Debezium, we recommend following the guidelines
outlined here.

### PostgreSQL settings

Materialize relies on Debezium's CDC envelope (which expresses changes to data)
to understand all CRUD-like operations happening in the upstream database.

For Debezium to emit the envelope Materialize expects when using PostgreSQL, the
tables which you're performing CDC on must have the parameter `REPLICA IDENTITY
FULL` set.

For example:

```sql
ALTER TABLE foo
REPLICA IDENTITY FULL;
```

### Kafka-less setup

Users who wish to connect a database to Materialize without having to use Kafka can
do so with [tb](https://github.com/MaterializeInc/tb). tb is a tool built by Materialize
that lets you send Debezium data to Materialize using Avro OCF files, bypassing the need
for Kafka. For more information and instructions, see
[tb's GitHub page](https://github.com/MaterializeInc/tb).
