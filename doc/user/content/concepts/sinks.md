---
title: Sinks
description: Learn about sinks in Materialize.
menu:
  main:
    parent: concepts
    weight: 25
    identifier: 'concepts-sinks'
aliases:
  - /get-started/key-concepts/#sinks
---

## Overview

Sinks are the inverse of sources and represent a connection to an external stream
where Materialize outputs data. When a user defines a sink over a materialized view,
source, or table, Materialize automatically generates the required schema and writes down
the stream of changes to that view or source. In effect, Materialize sinks act as
change data capture (CDC) producers for the given source or view.

Currently, Materialize only supports sending sink data to Kafka. See
the [Kafka sink documentation](/sql/create-sink/kafka) for details.

## Clusters and sinks

Avoid putting sinks on the same cluster that hosts sources.

See also [Operational guidelines](/manage/operational-guidelines/).

## Hydration considerations

During hydration, sinks need to load an entire snpshot of the data in memory.

## Related pages

- [`CREATE SINK`](/sql/create-sink)
