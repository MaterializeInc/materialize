---
title: "Concepts"
description: "Learn about the core concepts in Materialize."
disable_list: true
menu:
  main:
    parent: fundamentals
    weight: 10
    identifier: concepts
aliases:
  - /overview/api-components/
  - /overview/key-concepts/
  - /get-started/key-concepts/
  - /overview
  - /self-managed/v25.1/concepts/
  - /self-managed/v25.2/concepts/
  - /concepts/
---

The pages in this section introduces some of the key concepts in Materialize:

Concept                                  | Description
-----------------------------------------|-----
[Clusters](/fundamentals/concepts/clusters/)          | Clusters are isolated pools of compute resources for sources, sinks, indexes, materialized views, and ad-hoc queries.
[Sources](/fundamentals/concepts/sources/)            | Sources describe an external system you want Materialize to read data from.
[Views](/fundamentals/concepts/views/)    | Views represent a named query that you want to save for repeated execution. You can use **indexed views** and **materialized views** to incrementally maintain the results of views.
[Indexes](/fundamentals/concepts/indexes/)            | Indexes represent query results stored in memory.
[Arrangements](/fundamentals/concepts/arrangements/) | Arrangements are the in-memory data structures that maintain indexes and materialized views.
[Sinks](/fundamentals/concepts/sinks/)                | Sinks describe an external system you want Materialize to write data to.
[Snapshotting](/fundamentals/concepts/snapshotting/) | The initial sync of a source's data from an upstream system, before the source can serve queries.
[Hydration](/fundamentals/concepts/hydration/) | {{< include-from-yaml data="hydration-details" name="definition" >}}
[Reaction Time](/fundamentals/concepts/reaction-time) | Measures how quickly a system can reflect a change in input data and return an up-to-date query result. Defined as the sum of data freshness and query latency.

Refer to the individual pages for more information.
