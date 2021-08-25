This is an end-to-end test that spawns a Postgres -> Debezium -> Kafka -> Materialize pipeline
and then performs various operations on it, including DDL and schema migrations

To run:

./mzcompose down -v ; ./mzcompose run debezium-avro

The tests are numbered so that 9*.td run after all the others,
since they modify the Debezium configuration on the fly.
