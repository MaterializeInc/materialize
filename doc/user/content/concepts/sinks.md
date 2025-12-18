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

Sinks are the inverse of sources and represent a connection to an external
stream where Materialize outputs data. You can sink data from a **materialized**
view, a source, or a table.

## Sink methods

To create a sink, you can:

{{< yaml-table data="sink_external_systems" >}}

## Clusters and sinks

Avoid putting sinks on the same cluster that hosts sources.

See also [Operational guidelines](/manage/operational-guidelines/).

## Hydration considerations

During creation, Kafka sinks need to load an entire snapshot of the data in
memory.

## Related pages

- [`CREATE SINK`](/sql/create-sink)
