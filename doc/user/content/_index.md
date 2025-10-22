---
title: "Materialize Docs"
htmltitle: "Home"
disable_toc: true
disable_list: true
disable_h1: true
weight: 1
---

# Materialize documentation

Materialize is the live data layer for apps and AI agents. With Materialize, you
can use SQL to create up-to-the-second views on fast changing data.

{{< callout >}}

## What's new!

- [Self-managed Materialize v25.2], which includes support for password
  authentication (private preview), RBAC, and more! With Self-managed
  Materialize, you can deploy and operate Materialize in your Kubernetes
  environment. Self-managed Materialize is available in both Enterprise and
  Community editions. For more information, see the [Self-managed Materialize]
  documentation.

- [Materialize MCP
  Server](https://materialize.com/blog/materialize-turns-views-into-tools-for-agents/).
  The Materialize MCP Server bridges SQL and AI by transforming indexed views
  into well-typed tools that agents can call directly. It lets you expose your
  most complex logic as operational data products automatically and reliably.
  For more information, see the [Materialize MCP Server](/integrations/llm/)
  documentation.

[Self-managed Materialize]: https://materialize.com/docs/self-managed/v25.2/
[Self-managed Materialize v25.2]: https://materialize.com/docs/self-managed/v25.2/
{{</ callout >}}

{{< callout
primary_url="https://materialize.com/register/?utm_campaign=General&utm_source=documentation"
primary_text="Get Started">}}

## Ready to get started? 🚀

1. Sign up for a [free trial
   account](https://materialize.com/register/?utm_campaign=General&utm_source=documentation)
   on Materialize Cloud. Alternatively, both the [Materialize Emulator Docker
   image](/get-started/install-materialize-emulator/) and the [Self-managed
   Materialize] are also available.
2. Follow the quickstart guide to learn the basics.
3. Connect your own data sources and start building.

[Self-managed Materialize]: https://materialize.com/docs/self-managed/v25.2/
{{</ callout >}}

{{< multilinkbox >}}
{{< linkbox title="Key Concepts" >}}

-   [Materialize overview](/overview/what-is-materialize/)
-   [Clusters](/concepts/clusters/)
-   [Sources](/concepts/sources/)
-   [Views](/concepts/views/)
-   [Indexes](/concepts/indexes/)
-   [Sinks](/concepts/sinks/)

    {{</ linkbox >}}
    {{< linkbox title="Guides" >}}
-   [Materialize &amp; Postgres CDC](/integrations/cdc-postgres/)
-   [dbt &amp; Materialize](/integrations/dbt/)
-   [Materialize &amp; Node.js](/integrations/node-js/)

-   [Time-windowed computation](/sql/patterns/temporal-filters/)
    {{</ linkbox >}}
    {{< linkbox title="Reference" >}}
-   [`CREATE VIEW`](/sql/create-view/)
-   [`CREATE INDEX`](/sql/create-index/)
-   [`CREATE SOURCE`](/sql/create-source/)
-   [`SQL data types`](/sql/types/)
-   [`SQL functions`](/sql/functions/)
    {{</ linkbox >}}
    {{</ multilinkbox >}}

## Learn more

- To find out about new features and improvements, see also the
  [Changelog](https://materialize.com/changelog/).

- Check out the [**Materialize blog**](https://www.materialize.com/blog/) for
  the latest updates and Materialize deep-dives.
