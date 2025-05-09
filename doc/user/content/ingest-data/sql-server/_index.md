---
title: "SQL Server"
description: "Connecting Materialize to a SQL Server database for Change Data Capture (CDC)."
disable_list: true
menu:
  main:
    parent: 'ingest-data'
    identifier: 'sql-server'
    weight: 5
---

## Change Data Capture (CDC)

Materialize supports SQL Server as a real-time data source. The [SQL Server source](/sql/create-source/sql-server/)
uses SQL Server's [change data capture](/sql/create-source/sql-server/#change-data-capture) feature
to **continually ingest changes** resulting from CRUD operations in the upstream
database. The native support for SQL Server Change Data Capture (CDC) in Materialize
gives you the following benefits:

* **No additional infrastructure:** Ingest SQL Server change data into Materialize in
    real-time with no architectural changes or additional operational overhead.
    In particular, you **do not need to deploy Kafka and Debezium** for SQL Server
    CDC.

* **Transactional consistency:** The SQL Server source ensures that transactions in
    the upstream SQL Server database are respected downstream. Materialize will
    **never show partial results** based on partially replicated transactions.

* **Incrementally updated materialized views:** Materialized views are **not
    supported in MySQL**, so you can use Materialize as a
    read-replica to build views on top of your MySQL data that are efficiently
    maintained and always up-to-date.
