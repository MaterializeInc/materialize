# Debezium Tests

This is an end-to-end test that spawns a Database -> Debezium -> Kafka -> Materialize pipeline
and then performs various operations on it, including DDL and schema migrations.

Each database has its own workflow, see `./mzcompose list` for the complete list.

    ./mzcompose down -v ; ./mzcompose run DATABASE

The tests are numbered so that 9*.td run after all the others, since they modify the
Debezium configuration on the fly.
