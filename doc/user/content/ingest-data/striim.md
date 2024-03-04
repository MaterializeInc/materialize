---
title: "Striim"
description: "How to connect Striim to Materialize as a source."
aliases:
  - /integrations/striim/
menu:
  main:
    parent: "kafka"
    name: "Striim"
---

This guide walks through the steps to ingest data from Striim into Materialize using a [Kafka source](/sql/create-source/kafka/).

Striim is a real-time data integration platform that offers a variety of connectors for databases, messaging systems, and other data sources. This guide focuses on streaming data from MySQL to Kafka but can be adapted to other databases as well.

## Before you begin

Ensure you have the following:

- An active [Striim account](https://go2.striim.com/free-trial) and operational service.
- Configured MySQL, Oracle, or SQL Server with streamable tables.
- An operational Kafka cluster.
- Confluent Schema Registry set up for Avro schema management.

## Integration steps

### Database source configuration

1. **MySQL CDC Setup:**
    - Activate binary logging with `row` format in MySQL.
    - Follow Striim's [MySQL CDC guide](https://www.striim.com/docs/GCP/StriimForBigQuery/en/prerequisite-checks-mysql.html) for detailed setup instructions.

2. **Striim MySQLReader App Creation:**
    - Deploy an app within Striim, entering MySQL specifics and selecting target tables for streaming.

Alternatively, follow the [Oracle CDC guide](https://www.striim.com/docs/en/oracle-database-cdc.html) or [SQL Server CDC guide](https://www.striim.com/docs/en/sql-server-cdc.html) for database-specific integration steps.

### Kafka target configuration

1. **Kafka cluster verification:**
    - Create a [Kafka Writer](https://www.striim.com/docs/en/kafka-writer.html) within Striim to direct data to the Kafka cluster.
    - For the 'Input Stream' select the data source created in the previous step.
    - Specify the Kafka broker details and topic name.
    - Under the 'Advanced settings', in the 'Kafka Config' field, add the `value.serializer=io.confluent.kafka.serializers.KafkaAvroSerializer` configuration to encode records using the Confluent wire protocol.
    - Add a 'MESSAGE KEY' field to specify the primary key of the table. This will configure the KafkaWriter to emit the table’s primary key in the Kafka message key.

2. **Schema Registry configuration:**
    - From the 'Formatter' dropdown menu, select 'AvroFormatter' to encode records in the Avro format.
    - Define the Schema Registry URL.
    - Set the format to 'Table' or 'Native'. When using 'Table; as the format in the KafkaWriter configuration, it will strip out the unnecessary WAEvent metadata.
    - Configure the Schema Registry authentication credentials if necessary.
    - Leave the 'Schema Registry Subject Name' and 'Schema Registry Subject Name Mapping' fields empty.

### Start the Striim app

Once the Striim app is configured, start the app to begin streaming data from the database to Kafka.

You can monitor the app's progress and performance through Striim's web console.

The Striim app will stream data from the database to Kafka in Avro format, ready for Materialize to consume.

### Materialize configuration

1. In Materialize, create [Kafka and Confluent Schema Registry connections](/sql/create-connection/):

    ```sql
    CREATE SECRET kafka_password AS '...';
    CREATE SECRET csr_password AS '...';

    CREATE CONNECTION kafka_connection TO KAFKA (
        BROKER 'unique-jellyfish-0000-kafka.upstash.io:9092',
        SECURITY PROTOCOL = 'SASL_PLAINTEXT',
        SASL MECHANISMS = 'SCRAM-SHA-256', -- or `PLAIN` or `SCRAM-SHA-512`
        SASL USERNAME = 'foo',
        SASL PASSWORD = SECRET kafka_password
        SSH TUNNEL ssh_connection
    );

    CREATE CONNECTION csr_basic TO CONFLUENT SCHEMA REGISTRY (
        URL 'https://rp-f00000bar.data.vectorized.cloud:30993',
        USERNAME = 'foo',
        PASSWORD = SECRET csr_password
    );
    ```

2. Next, create a [Kafka source](/sql/create-source/kafka/):

    ```sql
    CREATE SOURCE src
        FROM KAFKA CONNECTION kafka_conn (TOPIC 'striim_topic')
        KEY FORMAT TEXT
        VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
        VALUE STRATEGY ID 1
        ENVELOPE UPSERT;
    ```

    The `VALUE STRATEGY ID 1` command manually specifies the schema ID, addressing Striim's nonstandard naming conventions in CSR.

    To get the schema ID, use the following command:

    ```sql
    curl <schema-registry-url>:8081/subjects/<schema-name>/versions/latest | jq .id
    ```


### Alternative approaches

#### JSON formatting

Striim’s JSON formatter offers an alternative to Avro, simplifying the configuration process as it does not require a Schema Registry.

Here's how you can leverage JSON formatting with Materialize:

1. **Striim configuration:**
   - Set up Striim to transmit events to Kafka in JSON format instead of Avro. This method simplifies the configuration compared to Avro, especially when dealing with complex schema evolution.

2. **Materialize JSON source creation:**

    ```sql
    CREATE SOURCE users
        FROM KAFKA CONNECTION k (TOPIC 'users')
        FORMAT JSON
        ENVELOPE NONE;
    ```

    This statement creates a source named `users` that reads from the Kafka topic `users` and expects the data to be in JSON format.

3. **Data transformation in Materialize:**

    ```sql
    SELECT
        (r->'data'->>'id')::int4 AS id,
        (r->'data'->>'email')::text AS email,
        (r->'data'->>'is_vip')::boolean AS is_vip,
        (r->'data'->>'created_at')::timestamp AS created_at,
        (r->'data'->>'updated_at')::timestamp AS updated_at
    FROM users, jsonb_array_elements(data) r;
    ```

    This statement transforms the JSON data into a tabular format, allowing you to query and analyze the data.

    {{< json-parser >}}

## Database specifics

### Oracle

- Oracle versions >= 19c do not support CDDL. Utilize the [Oracle CDC guide](https://www.striim.com/docs/en/oracle-database-cdc.html) for alternative configuration strategies.

### SQL Server

- Implement the [SQL Server CDC guide](https://www.striim.com/docs/en/sql-server-cdc.html) for integration steps specific to SQL Server environments.

## Known issues

- `BigInt` columns are not supported in Striim's Avro handling. This issue is being addressed by Striim's development team.

## Related pages

- [`CREATE SOURCE`](/sql/create-source/kafka/)
- [`CREATE CONNECTION`](/sql/create-connection/)
