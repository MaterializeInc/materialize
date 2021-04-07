

## Reading Messages Using Kafkacat

Now that you've started the demo, you can read the raw messages from Kafka using kafkacat:

```sh
kafkacat -C -b localhost:9092 -s avro -r localhost:8081 -t debezium.inventory.customers -e
{"id": 1001}{"before": null, "after": {"Value": {"id": 1001, "first_name": "Sally", "last_name": "Thomas", "email": "sally.thomas@acme.com"}}, "source": {"version": "1.4.2.Final", "connector": "mysql", "name": "debezium", "ts_ms": 0, "snapshot": {"string": "true"}, "db": "inventory", "table": {"string": "customers"}, "server_id": 0, "gtid": null, "file": "mysql-bin.000003", "pos": 154, "row": 0, "thread": null, "query": null}, "op": "r", "ts_ms": {"long": 1617830680716}, "transaction": null}
{"id": 1002}{"before": null, "after": {"Value": {"id": 1002, "first_name": "George", "last_name": "Bailey", "email": "gbailey@foobar.com"}}, "source": {"version": "1.4.2.Final", "connector": "mysql", "name": "debezium", "ts_ms": 0, "snapshot": {"string": "true"}, "db": "inventory", "table": {"string": "customers"}, "server_id": 0, "gtid": null, "file": "mysql-bin.000003", "pos": 154, "row": 0, "thread": null, "query": null}, "op": "r", "ts_ms": {"long": 1617830680717}, "transaction": null}
{"id": 1003}{"before": null, "after": {"Value": {"id": 1003, "first_name": "Edward", "last_name": "Walker", "email": "ed@walker.com"}}, "source": {"version": "1.4.2.Final", "connector": "mysql", "name": "debezium", "ts_ms": 0, "snapshot": {"string": "true"}, "db": "inventory", "table": {"string": "customers"}, "server_id": 0, "gtid": null, "file": "mysql-bin.000003", "pos": 154, "row": 0, "thread": null, "query": null}, "op": "r", "ts_ms": {"long": 1617830680717}, "transaction": null}
{"id": 1004}{"before": null, "after": {"Value": {"id": 1004, "first_name": "Anne", "last_name": "Kretchmar", "email": "annek@noanswer.org"}}, "source": {"version": "1.4.2.Final", "connector": "mysql", "name": "debezium", "ts_ms": 0, "snapshot": {"string": "true"}, "db": "inventory", "table": {"string": "customers"}, "server_id": 0, "gtid": null, "file": "mysql-bin.000003", "pos": 154, "row": 0, "thread": null, "query": null}, "op": "r", "ts_ms": {"long": 1617830680717}, "transaction": null}
% Reached end of topic debezium.inventory.customers [0] at offset 4: exiting
```

## Importing Tables in Materialized

If you want to import the tables by hand, you can use the following SQL statement:

```sql
CREATE MATERIALIZED SOURCE IF NOT EXISTS customers
FROM KAFKA BROKER 'kafka:9092' TOPIC 'debezium.inventory.customers'
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081'
ENVELOPE DEBEZIUM;
```
