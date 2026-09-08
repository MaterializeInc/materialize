---
title: "Serve results"
description: "Serving results from Materialize"
disable_list: true
menus:
  main:
    weight: 50
    identifier: 'serve-results'
aliases:
  - /self-managed/v25.2/serve-results/
  - /self-managed/v25.2/serve-results/query-results/
  - /self-managed/v25.2/serve-results/bi-tools/
---

In Materialize, indexed views and materialized views maintain up-to-date query
results. This allows Materialize to serve fresh query results with low latency.

To serve results, you can:

- [Query using `SELECT` and `SUBSCRIBE`
  statements](/serve-results/query-results/)

- [Use BI/data collaboration tools](/serve-results/bi-tools/)

- [Sink results to to external systems](/export-data/)

- [Use Foreign Data Wrapper (FDW)](/serve-results/fdw/)

{{< multilinkbox >}}
{{< linkbox title="SELECT/SUBSCRIBE statements" >}}
- [Query using `SELECT` and `SUBSCRIBE`](/serve-results/query-results/)
- [Use Foreign Data Wrapper (FDW)](/serve-results/fdw/)
{{</ linkbox >}}
{{< linkbox title="External BI tools" >}}
- [Deepnote](/serve-results/bi-tools/deepnote/)
- [Excel](/serve-results/bi-tools/excel/)
- [Hex](/serve-results/bi-tools/hex/)
- [Metabase](/serve-results/bi-tools/metabase/)
- [Power BI](/serve-results/bi-tools/power-bi/)
- [Tableau](/serve-results/bi-tools/tableau/)
- [Looker](/serve-results/bi-tools/looker/)
{{</ linkbox >}}

{{< linkbox title="Sink results" >}}
- [Sinking results to Amazon S3](/export-data/s3/)
- [Sinking results to Census](/export-data/census/)
- [Sinking results to Kafka](/export-data/kafka/)
- [Sinking results to Snowflake](/export-data/snowflake/)
{{</ linkbox >}}

{{</ multilinkbox >}}
