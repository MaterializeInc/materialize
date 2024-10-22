# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ schema-registry-wait topic=mysql.test.t1

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
USE test;
INSERT INTO t1 VALUES (345, 345, 345);
COMMIT;

$ schema-registry-wait topic=mysql.transaction

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE SOURCE t1
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'mysql.test.t1');

> CREATE TABLE t1_tbl FROM SOURCE t1 (REFERENCE "mysql.test.t1")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;

> SELECT * FROM t1_tbl;
123 123 123
234 234 234
345 345 345

$ mysql-execute name=mysql
INSERT INTO t1 VALUES (456, 456, 456);
COMMIT;

> SELECT * FROM t1_tbl;
123 123 123
234 234 234
345 345 345
456 456 456

$ mysql-execute name=mysql
UPDATE t1 SET f2 = f2 * 100
COMMIT;

> SELECT * FROM t1_tbl;
123 12300 123
234 23400 234
345 34500 345
456 45600 456

$ mysql-execute name=mysql
DELETE FROM t1;
COMMIT;

> SELECT COUNT(*) FROM t1_tbl;
0
