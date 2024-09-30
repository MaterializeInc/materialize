---
title: "Overview"
description: "Connect and ingest data from MySQL."
disable_list: true
menu:
  main:
    parent: 'mysql'
    weight: 1
    identifier: 'mysql-overview'
---

### Native MySQL Connectors

Materialize provides native support for MySQL (5.7+) as a source.

{{< private-preview />}}


{{< multilinkbox >}}
{{< linkbox title="MySQL Connector (Preview)" >}}

- [Amazon Aurora](/ingest-data/mysql/amazon-aurora/)

{{</ linkbox >}}

{{< linkbox title="MySQL Connector (Preview)" >}}

- [Amazon RDS](/ingest-data/mysql/amazon-rds/)

{{</ linkbox >}}


{{< linkbox title="MySQL Connector (Preview)" >}}

- [Azure DB](/ingest-data/mysql/azure-db/)

{{</ linkbox >}}


{{</ multilinkbox >}}

{{< multilinkbox >}}
{{< linkbox title="MySQL Connector (Preview)" >}}

- [Google Cloud SQL](/ingest-data/mysql/google-cloud-sql/)

{{</ linkbox >}}
{{< linkbox title="MySQL Connector (Preview)" >}}

- [Self-hosted MySQL](/ingest-data/mysql/self-hosted/)

{{</ linkbox >}}

{{</ multilinkbox >}}


### Support via Kafka and Debezium

{{< tip >}}

If possible, use Materialize's [native support for MySQL](#native-mysql-connectors) instead.

{{< /tip >}}

Materialize also supports the use of [Debezium  and
Kafka](/integrations/cdc-mysql/) to propagate Change Data Capture (CDC) data
from MySQL to Materialize.
