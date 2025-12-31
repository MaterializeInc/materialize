---
audience: developer
canonical_url: https://materialize.com/docs/concepts/sinks/
complexity: beginner
description: Learn about sinks in Materialize.
doc_type: concept
keywords:
- Sinks
- CREATE SINK
- materialized
- CREATE A
product_area: Concepts
status: stable
title: Sinks
---

# Sinks

## Purpose
Learn about sinks in Materialize.

Read this to understand how this concept works in Materialize.


Learn about sinks in Materialize.



## Overview

Sinks are the inverse of sources and represent a connection to an external
stream where Materialize outputs data. You can sink data from a **materialized**
view, a source, or a table.

## Sink methods

To create a sink, you can:

<!-- Dynamic table: sink_external_systems - see original docs -->

## Clusters and sinks

Avoid putting sinks on the same cluster that hosts sources.

See also [Operational guidelines](/manage/operational-guidelines/).

## Hydration considerations

During creation, Kafka sinks need to load an entire snapshot of the data in
memory.

## Related pages

- [`CREATE SINK`](/sql/create-sink)

