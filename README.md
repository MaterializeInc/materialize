# Material

Material provides streaming SQL materialized views on top of
[differential dataflow].

⚠️ Material is a work in progress. Expect things to break frequently. ⚠️

[differential dataflow]: https://github.com/timelydata/differential-dataflow

## Basic demo instruction

Optional: use nix to install and start all dependencies:

```bash
cd materialize
nix-shell
confluent start schema-registry
```

Assuming you have ZooKeeper, Kafka, and the Confluent Schema Registry running
on your local machine on the default ports:

```bash
zookeeper-shell localhost:2181 <<< 'rmr /materialized'
kafka-topics --zookeeper localhost:2181 --delete --topic quotes
curl -X DELETE http://localhost:8081/subjects/quotes-value
confluent status
kafka-avro-console-producer --topic quotes --broker-list localhost:9092 --property value.schema='{"type": "record", "name": "na", "fields": [{"name": "quote", "type": "string"}]}'
# Type in some quotes. (There won't be a prompt. Type anyway.)
# {"quote": "Syntax highlighting is juvenile. —Rob Pike"}
# {"quote": "Arrogance in computer science is measured in nano-Dijkstras. —Alan Kay"}
```

```bash
cargo run
```

Then, in another shell:

```bash
$ psql -h localhost -p 6875 sslmode=disable
> CREATE DATA SOURCE quotes FROM 'kafka://localhost/quotes' USING SCHEMA '{"type": "record", "name": "na", "fields": [{"name": "quote", "type": "string"}]}';
> PEEK quotes;
> CREATE MATERIALIZED VIEW business_insights AS SELECT quote, 42 FROM quotes;
> PEEK business_insights;
```

## Aggregate demo

```bash
$ kafka-avro-console-producer --topic aggdata --broker-list localhost:9092 --property value.schema='{"type": "record", "name": "na", "fields": [{"name": "a", "type": "long"}, {"name": "b", "type": "long"}]}'
# {"a": 1, "b": 1}
# {"a": 2, "b": 1}
# {"a": 3, "b": 1}
# {"a": 1, "b": 2}

$ psql -h localhost -p 6875 sslmode=disable
> CREATE DATA SOURCE aggdata FROM 'kafka://localhost/aggdata' USING SCHEMA '{"type": "record", "name": "na", "fields": [{"name": "a", "type": "long"}, {"name": "b", "type": "long"}]}';
> CREATE MATERIALIZED VIEW aggtest AS SELECT sum(a) FROM aggdata GROUP BY b;
> PEEK aggtest;
```

## Join demo

```bash
$ kafka-avro-console-producer --topic src1 --broker-list localhost:9092 --property value.schema='{"type": "record", "name": "na", "fields": [{"name": "a", "type": "long"}, {"name": "b", "type": "long"}]}'
# {"a": 1, "b": 1}
# {"a": 2, "b": 1}
# {"a": 1, "b": 2}
# {"a": 1, "b": 3}

$ kafka-avro-console-producer --topic src2 --broker-list localhost:9092 --property value.schema='{"type": "record", "name": "na", "fields": [{"name": "c", "type": "long"}, {"name": "d", "type": "long"}]}'
# {"c": 1, "d": 1}
# {"c": 1, "d": 2}
# {"c": 1, "d": 3}
# {"c": 3, "d": 1}

$ psql -h localhost -p 6875 sslmode=disable
> CREATE DATA SOURCE src1 FROM 'kafka://localhost/src1' USING SCHEMA '{"type": "record", "name": "na", "fields": [{"name": "a", "type": "long"}, {"name": "b", "type": "long"}]}';
> CREATE DATA SOURCE src2 FROM 'kafka://localhost/src2' USING SCHEMA '{"type": "record", "name": "na", "fields": [{"name": "c", "type": "long"}, {"name": "d", "type": "long"}]}';
> CREATE MATERIALIZED VIEW jointest AS SELECT a, b, d FROM src1 JOIN src2 ON c = b;
> PEEK jointest;
```
