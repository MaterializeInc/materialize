---
title: "ALTER SINK"
description: "`ALTER SINK` changes the provisioned size of a sink."
menu:
  main:
    parent: 'commands'
---

`ALTER SINK` changes the provisioned [size](/sql/create-sink/#sizing-a-sink) of a sink.

## Syntax

{{< diagram "alter-sink.svg" >}}

Field   | Use
--------|-----
_name_  | The identifier of the sink you want to alter.
_value_ | The new value for the sink size. Accepts values: `3xsmall`, `2xsmall`, `xsmall`, `small`, `medium`, `large`.

## See also

- [`CREATE SINK`](/sql/create-sink/)
- [`SHOW SINKS`](/sql/show-sinks)
