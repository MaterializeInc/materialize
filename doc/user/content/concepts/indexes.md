---
title: Indexes
description: Conceptual page on indexes.
menu:
  main:
    parent: concepts
    weight: 20
    identifier: 'concepts-indexes'
---

## Overview

Indexes assemble and maintain a query's results in memory within a
[cluster](/concepts/clusters/), which provides future queries
the data they need in a format they can immediately use.

These continually updated indexes are known as
_arrangements_ within Materialize's dataflows. In the simplest case, the
arrangement is the last operator and simply stores the query's output in
memory. In more complex cases, arrangements let Materialize perform
sophisticated operations like joins more efficiently.

For a deeper dive into how indexes work, see [Arrangements](/overview/arrangements/).

## Related pages

- [Views](/concepts/views)
- [`CREATE INDEX`](/sql/create-index)
