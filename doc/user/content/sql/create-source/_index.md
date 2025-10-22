---
title: "CREATE SOURCE"
description: "CREATE SOURCE reference page"
disable_list: true
menu:
  main:
    parent: 'commands'
    identifier: 'create-source'
---

{{< source-versioning-disambiguation is_new=true other_ref="[old reference page](/sql/create-source-v1/)" >}}

Creates a new source that connects to an upstream/external system. Once a new
source is created, you can [`CREATE TABLE`](/sql/create-table/) from the source
to start the data ingestion process. For Webhook populated tables, you do not
create a source but directly [create the table for the
webhook](/sql/create-table/webhook/).

## Syntax summary

Materialize can create sources from the following:

{{< tabs >}}
{{< tab "Kafka" >}}
{{% include-example file="examples/create_source/example_kafka_source"
 example="syntax" %}}

For details, see [CREATE SOURCE: Kafka](/sql/create-source/Kafka/).
{{< /tab >}}
{{< tab "MySQL" >}}

{{% include-example file="examples/create_source/example_mysql_source"
 example="syntax" %}}

For details, see [CREATE SOURCE: MySQL](/sql/create-source/mysql/).
{{< /tab >}}
{{< tab "PostgreSQL" >}}

{{% include-example file="examples/create_source/example_postgres_source"
 example="syntax" %}}

For details, see [CREATE SOURCE: PostgreSQL](/sql/create-source/postgres/).

{{< /tab >}}

{{< tab "SQL Server" >}}

{{% include-example file="examples/create_source/example_sql_server_source"
 example="syntax" %}}

For details, see [CREATE SOURCE: SQL Server](/sql/create-source/sql-server/).
{{< /tab >}}
{{< /tabs >}}

See also: For Webhook populated tables, see [CREATE TABLE: Webhook](/sql/create-table/webhook/).

## Considerations

### Sources and clusters

For the sources, you need to associate a cluster to provide the compute
resources.

{{< tip >}}
If possible, dedicate a cluster just for sources.
{{</ tip >}}
