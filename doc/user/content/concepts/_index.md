---
title: "Concepts"
description: "Learn about the core concepts in Materialize."
disable_list: true
menu:
  main:
    parent: get-started
    weight: 25
    identifier: concepts
aliases:
  - /overview/api-components/
  - /overview/key-concepts/
  - /get-started/key-concepts/
  - /overview
---

The pages in this section introduces some of the key concepts in Materialize:

Component                                | Use
-----------------------------------------|-----
[Clusters](/concepts/clusters/)          | Clusters are isolated pools of compute resources for sources, sinks, indexes, materialized views, and ad-hoc queries.
[Sources](/concepts/sources/)            | Sources describe an external system you want Materialize to read data from.
[Views](/concepts/views/)    | Views represent a named query that you want to save for repeated execution. You can use **indexed views** and **materialized views** to incrementally maintain the results of views.
[Indexes](/concepts/indexes/)            | Indexes represent query results stored in memory.
[Sinks](/concepts/sinks/)                | Sinks describe an external system you want Materialize to write data to.

Refer to the individual pages for more information.
