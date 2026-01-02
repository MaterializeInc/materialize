---
title: Glossary
description: Definitions of Materialize-specific terms
doc_type: reference
product_area: General
---

# Glossary

This glossary defines key Materialize-specific terms.

## A

**Arrangement**: An indexed, incrementally maintained collection stored in memory. Arrangements enable fast point lookups and are the foundation of Materialize's incremental view maintenance. See [Indexes](concepts/indexes/index.md).

## C

**Cluster**: A pool of isolated compute resources that runs dataflows. Each cluster has one or more replicas. See [Clusters](concepts/clusters/index.md).

**Connection**: A reusable object that stores credentials and configuration for connecting to external systems. See [CREATE CONNECTION](sql/create-connection/index.md).

## D

**Dataflow**: A computation graph that processes streaming data incrementally. Dataflows are created automatically when you create sources, materialized views, or indexes.

## I

**Index**: An arrangement on a view that enables fast point lookups. See [CREATE INDEX](sql/create-index/index.md).

## M

**Materialized View**: A view whose results are persisted and incrementally updated as source data changes. See [CREATE MATERIALIZED VIEW](sql/create-materialized-view/index.md).

## R

**Replica**: A copy of a cluster's compute resources. Multiple replicas provide fault tolerance.

## S

**Schema**: A namespace within a database that contains tables, views, and other objects. See [Namespaces](concepts/namespaces/index.md).

**Sink**: A connection that exports data from Materialize to an external system. See [CREATE SINK](sql/create-sink/index.md).

**Source**: A connection to an external system that streams data into Materialize. See [CREATE SOURCE](sql/create-source/index.md).

## V

**View**: A named query that is executed when referenced. Unlike materialized views, regular views do not persist results. See [CREATE VIEW](sql/create-view/index.md).
