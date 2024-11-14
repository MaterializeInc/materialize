---
title: "ALTER INDEX"
description: "`ALTER INDEX` changes the parameters of an index."
menu:
  main:
    parent: 'commands'
---

`ALTER INDEX` changes the parameters of an index.

## Syntax

{{< diagram "alter-index-set.svg" >}}
{{< diagram "alter-index-reset.svg" >}}

Field | Use
------|-----
_name_ | The identifier of the index you want to alter.
_retention_period_ | ***Private preview.** This option has known performance or stability issues and is under active development.* <br>Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period).  **Note:** Configuring indexes to retain history is not recommended. As an alternative, consider creating a materialized view for your subscription query and configuring the history retention period on the view instead. See [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). <br>Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). <br>Default: `1s`.

## Details

#### Tables

Note that when enabling indexes on tables, the first index you enable must be
the table's primary index, which was created at the same time as the table
itself. Only after enabling the primary index can you enable any secondary
indexes.

## Privileges

The privileges required to execute this statement are:

- Ownership of the index.

## Related pages

- [`SHOW INDEXES`](/sql/show-indexes)
- [`SHOW CREATE VIEW`](/sql/show-create-view)
- [`SHOW VIEWS`](/sql/show-views)
- [`SHOW SOURCES`](/sql/show-sources)
- [`SHOW SINKS`](/sql/show-sinks)
