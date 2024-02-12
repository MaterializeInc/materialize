---
title: "SSH tunnel connections"
description: "How to connect Materialize to a Kafka broker, or a PostgreSQL database using an SSH tunnel connection to a SSH bastion server"
menu:
  main:
    parent: "network-security"
    name: "SSH tunnel connections"
aliases:
  - /integrations/postgres-bastion/
  - /ops/network-security/ssh-tunnel/
  - /connect-sources/ssh-tunnel/


---

{{% network-security/ssh-tunnel %}}

## Create a source connection

In Materialize, create a source connection that uses the SSH tunnel connection you configured in the previous section:

{{< tabs tabID="1" >}}
{{< tab "Kafka">}}
```sql
CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'broker1:9092',
    SSH TUNNEL ssh_connection
);
```

You can reuse this Kafka connection across multiple [`CREATE SOURCE`](/sql/create-source/kafka/)
statements:

```sql
CREATE SOURCE json_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT JSON;
```

{{< /tab >}}
{{< tab "PostgreSQL">}}
```sql
CREATE SECRET pgpass AS '<POSTGRES_PASSWORD>';

CREATE CONNECTION pg_connection TO POSTGRES (
  HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
  PORT 5432,
  USER 'postgres',
  PASSWORD SECRET pgpass,
  SSL MODE 'require',
  DATABASE 'postgres'
  SSH TUNNEL ssh_connection
);
```

You can reuse this PostgreSQL connection across multiple [`CREATE SOURCE`](/sql/create-source/postgres/)
statements:

```sql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR ALL TABLES;
```
{{< /tab >}} {{< /tabs >}}

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`: Kafka](/sql/create-source/kafka/)
- [`CREATE SOURCE`: PostgreSQL](/sql/create-source/postgres/)
