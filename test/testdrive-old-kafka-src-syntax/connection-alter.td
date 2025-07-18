# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=60s
$ set-arg-default single-replica-cluster=quickstart
#
# Test basic connection functionality

$ skip-consistency-checks reason="workflow uses SSH keys which we currently can't check"

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_connection_validation_syntax = true

# Create working connection + some data to ingest
$ kafka-create-topic topic=connection_test partitions=1
$ kafka-ingest format=bytes topic=connection_test
1,2

> CREATE CONNECTION conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)

> SELECT name, type from mz_connections WHERE id LIKE 'u%'
name       type
------------------------------
conn   kafka

> CREATE CONNECTION progress_override TO KAFKA (
    BROKER '${testdrive.kafka-addr}',
    PROGRESS TOPIC 'some_progress_topic',
    SECURITY PROTOCOL PLAINTEXT
  )

> CREATE SOURCE mz_source (first, second)
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION conn (TOPIC 'testdrive-connection_test-${testdrive.seed}')
  FORMAT CSV WITH 2 COLUMNS

> SELECT * FROM mz_source
first second
------------
1     2

! ALTER CONNECTION conn RESET (broker);
contains:invalid ALTER CONNECTION: invalid CONNECTION: must set one of BROKER, BROKERS, or AWS PRIVATELINK

! ALTER CONNECTION conn SET (broker = 'abcd') WITH (validate = true);
contains:Failed to resolve hostname

> ALTER CONNECTION conn SET (broker = 'abcd') WITH (validate = false);

> SELECT status = 'stalled' FROM mz_internal.mz_source_statuses WHERE name = 'mz_source';
true

$ kafka-ingest format=bytes topic=connection_test
2,3

> ALTER CONNECTION conn SET (broker = '${testdrive.kafka-addr}') WITH (validate = true);

> SELECT * FROM mz_source
first second
------------
1     2
2     3

> ALTER CONNECTION conn SET (BROKERS ['${testdrive.kafka-addr}'])

! ALTER CONNECTION conn DROP (SECURITY PROTOCOL);
contains:Broker does not support SSL connections

! ALTER CONNECTION conn SET (BROKER '${testdrive.kafka-addr}'), RESET (BROKER);
contains:cannot both SET and DROP/RESET options BROKER

! ALTER CONNECTION conn SET (BROKER '${testdrive.kafka-addr}'), SET (BROKER '${testdrive.kafka-addr}');
contains:BROKER specified more than once

! ALTER CONNECTION conn SET (BROKER '${testdrive.kafka-addr}'), SET (BROKER '${testdrive.kafka-addr}'), SET (BROKER '${testdrive.kafka-addr}');
contains:BROKER specified more than once

! ALTER CONNECTION conn SET (BROKER '${testdrive.kafka-addr}'), SET (BROKERS ['${testdrive.kafka-addr}'])
contains:invalid ALTER CONNECTION: invalid CONNECTION: can only set one of BROKER, BROKERS, or AWS PRIVATELINK

! ALTER CONNECTION conn SET (BROKER '${testdrive.kafka-addr}'), DROP (BROKERS)
contains:cannot both SET and DROP/RESET mutually exclusive KAFKA options BROKER, BROKERS

! ALTER CONNECTION conn DROP (BROKER), SET (BROKERS ['${testdrive.kafka-addr}'])
contains:cannot both SET and DROP/RESET mutually exclusive KAFKA options BROKER, BROKERS

# We permit resetting both of these options, and the error occurs later in planning
! ALTER CONNECTION conn RESET (BROKER), RESET (BROKERS);
contains:invalid ALTER CONNECTION: invalid CONNECTION: must set one of BROKER, BROKERS, or AWS PRIVATELINK

> ALTER CONNECTION conn SET (BROKER = '${testdrive.kafka-addr}');

! ALTER CONNECTION conn SET (PROGRESS TOPIC = 'new');
contains:cannot ALTER KAFKA option PROGRESS TOPIC

! ALTER CONNECTION conn RESET (PROGRESS TOPIC);
contains:cannot ALTER KAFKA option PROGRESS TOPIC

# Misc. ALTER CONNECTION operations

! ALTER CONNECTION conn RESET (PROGRESS TOPIC), ROTATE KEYS;
contains:cannot specify any other actions alongside ALTER CONNECTION...ROTATE KEYS

! ALTER CONNECTION conn ROTATE KEYS, RESET (PROGRESS TOPIC);
contains:cannot specify any other actions alongside ALTER CONNECTION...ROTATE KEYS

! ALTER CONNECTION conn ROTATE KEYS, ROTATE KEYS;
contains:cannot specify any other actions alongside ALTER CONNECTION...ROTATE KEYS

# IF EXISTS
> ALTER CONNECTION IF EXISTS abc RESET (PROGRESS TOPIC);

## Mix altering option w/ rotate keys allowed iff in an IF EXISTS and object DNE
> ALTER CONNECTION IF EXISTS abc RESET (PROGRESS TOPIC), ROTATE KEYS;

# Rotate keys on non-SSH conn
! ALTER CONNECTION conn ROTATE KEYS
contains:is not an SSH connection

> CREATE CONNECTION ssh_conn TO SSH TUNNEL (
    HOST 'host',
    USER 'user',
    PORT 1
  )
  WITH (VALIDATE = false);

! ALTER CONNECTION ssh_conn ROTATE KEYS WITH (VALIDATE = true, VALIDATE = false, VALIDATE = 'abc');
contains:ALTER CONNECTION...ROTATE KEYS does not support WITH (VALIDATE = true, VALIDATE = false, VALIDATE = 'abc')
