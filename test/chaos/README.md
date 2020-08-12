### Chaos

Tests in this crate chaos test Materialize. Each test is implemented as an `mzconduct`
workflow and can be run as follows:

```shell script
bin/mzconduct up chaos -w test-bytes-to-kafka
```

In this command, `chaos` indicates the name of the target `mzcompose.yml` file and `test-bytes-to-kafka`
indicates the name of the target chaos test.

These tests can be run:
1. Locally using the above command or some variation
2. On EC2 instances via our `infra` terraform commands

### Existing Tests

- `test-bytes-to-kafka`: In this test, simple records are pushed directly to Kafka and
   subsequently read by Materialize.

- `test-mysql-debezium-kafka`: In this test, chbench data will be loaded into a MySQL
   database. As the data changes, Debezium will push those changes to Kafka. Materialize
   will read in the changes from Kafka.
