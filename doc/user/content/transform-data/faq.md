---
title: "FAQ: Indexes"
description: "Frequently asked questions about indexes."
menu:
  main:
    name: "FAQ: Indexes"
    identifier: faq-indexes
    parent: transform-data
    weight: 100
aliases:
  - /self-managed/v25.2/transform-data/faq/
---

## Are indexes in Materialize optimized for `ORDER BY`?

No.

{{% include-from-yaml data="index_details" name="index-key-distribution" %}}

{{% include-from-yaml data="index_details" name="index-key-ordering-within-workers" %}}

As such, Materialize indexes are not optimized for ordered access, including
`ORDER BY` clauses.

## Are indexes in Materialize optimized for range queries?

No.

{{% include-from-yaml data="index_details" name="index-key-distribution" %}}

{{% include-from-yaml data="index_details" name="index-key-ordering-within-workers" %}}

As such, Materialize indexes are not optimized for ordered access, including
range queries.

## Are indexes in Materialize optimized for `GROUP BY` aggregations?

No. {{< include-from-yaml data="index_details" name="index-groupby-not-optimized" >}}
