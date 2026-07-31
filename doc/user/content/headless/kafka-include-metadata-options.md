---
headless: true
---
| Option | Description |
|--------|-------------|
| **KEY [AS \<name\>]** | Include a column containing the Kafka message key. If the key is encoded using a format that includes schemas, the column will take its name from the schema. For unnamed formats (e.g. `TEXT`), the column will be named `key`. The column can be renamed with the optional **AS** *name* statement.
| **PARTITION [AS \<name\>]** | Include a `partition` column containing the Kafka message partition. The column can be renamed with the optional **AS** *name* clause.
| **OFFSET [AS \<name\>]** | Include an `offset` column containing the Kafka message offset. The column can be renamed with the optional **AS** *name* clause.
| **TIMESTAMP [AS \<name\>]** | Include a `timestamp` column containing the Kafka message timestamp. The column can be renamed with the optional **AS** *name* clause. <br><br>Note that the timestamp of a Kafka message depends on how the topic and its producers are configured. See the [Confluent documentation](https://docs.confluent.io/3.0.0/streams/concepts.html?#time) for details.
| **HEADERS [AS \<name\>]** | Include a `headers` column containing the Kafka message headers as a list of records of type `(key text, value bytea)`. The column can be renamed with the optional **AS** *name* clause.
| **HEADER \<key\> AS \<name\> [**BYTES**]** | Include a *name* column containing the Kafka message header *key* parsed as a UTF-8 string. To expose the header value as `bytea`, use the `BYTES` option.
