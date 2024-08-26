---
title: "PostgreSQL"
description: "Connect and ingest data from PostgreSQL."
---

The following section contains documentation on connecting and ingesting data
from PostgreSQL.

### Native support for PostgreSQL

Materialize provides native support for the following PostgreSQL:

|                          |
| ------------------------------------------- |
| {{% ingest-data/postgres-native-support %}} |

### Support via Kafka and Debezium

{{< tip >}}

If possible, use Materialize's [native support for
PostgreSQL](#native-support-for-postgresql) instead.

{{</ tip >}}

You can use Debezium and the Kafka source to propagate Change Data Capture (CDC)
data from PostgreSQL to Materialize.

| Debezium + Kafka                            |
| ------------------------------------------- |
| [PostgreSQL CDC using Kafka and Debezium](/ingest-data/postgres/postgres-debezium/) |

### Section contents
