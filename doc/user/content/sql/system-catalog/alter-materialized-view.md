---
title: "ALTER MATERIALIZED VIEW"
description: "`ALTER MATERIALIZED VIEW` changes the parameters of a materialized view."
menu:
  main:
    parent: 'commands'
---

`ALTER MATERIALIZED VIEW` changes the parameters of a materialized view.

## Syntax

{{< diagram "alter-materialized-view.svg" >}}

Field | Use
------|-----
_name_ | The identifier of the materialized view you want to alter.
_retention_period_ | ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data for performing time travel actions. Accepts positive [interval](https://materialize.com/docs/sql/types/interval/) values like `'1hr'`. See [history retention period](/manage/history-rentention-period) guide. Default is one second.

## Details
