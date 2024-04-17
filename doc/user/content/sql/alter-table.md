---
title: "ALTER TABLE"
description: "`ALTER TABLE` changes the parameters of a table."
menu:
  main:
    parent: 'commands'
---

`ALTER TABLE` changes the parameters of a table.

## Syntax

{{< diagram "alter-table.svg" >}}

Field | Use
------|-----
_name_ | The identifier of the table you want to alter.
_retention_period_ | ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data for performing time travel actions (see [History retention period](/manage/history-rentention-period)). Accepts positive [interval](https://materialize.com/docs/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`.

## Details
