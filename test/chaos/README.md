### Chaos

This crate is intended to provide tools and tests to chaos test Materialize. The tests are
implemented as `mzconduct` workflows, meaning that they can be run as follows:

```shell script
bin/mzconduct up chaos -w delay-kafka
```

In this command, `chaos` indicates the name of the target `mzcompose.yml` file and `delay-kafka`
indicates the name of the target test. Each test has its own unique workflow.

These tests can be run:
1. Locally via the above command
2. On EC2 instances via our `infra` terraform commands

### Existing Tests

For the configuration:

- Bytes -> Kafka -> Materialize:
    - basic Docker chaos tests (pause, stop, kill)
    - basic network chaos tests (delay, rate, limit, loss, corrupt, duplicate)

- Mysql -> Debezium -> Kafka -> Materialize:
    - basic Docker chaos tests (pause, stop, kill)
    - basic network chaos tests (delay, rate, limit, loss, corrupt, duplicate)

All other tests are yet to be created and added.
