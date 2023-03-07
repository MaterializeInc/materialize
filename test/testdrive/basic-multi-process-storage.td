# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests that multi-process storage objects (in this case, sources)
# actually utilize all the processes in the replica.

# Create a topic with enough partitions and data spread
# among them to have a very, very high likelihood
# to distribute at least 1 message to each worker.
$ kafka-create-topic topic=many partitions=10
$ kafka-ingest format=bytes topic=many partition=0
msg
$ kafka-ingest format=bytes topic=many partition=1
msg
$ kafka-ingest format=bytes topic=many partition=2
msg
$ kafka-ingest format=bytes topic=many partition=3
msg
$ kafka-ingest format=bytes topic=many partition=4
msg
$ kafka-ingest format=bytes topic=many partition=5
msg
$ kafka-ingest format=bytes topic=many partition=6
msg
$ kafka-ingest format=bytes topic=many partition=7
msg
$ kafka-ingest format=bytes topic=many partition=8
msg
$ kafka-ingest format=bytes topic=many partition=9
msg

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE SOURCE many
  FROM KAFKA CONNECTION kafka_conn (TOPIC
  'testdrive-many-${testdrive.seed}'
  )
  FORMAT BYTES
  WITH (
    SIZE '2-2'
  )

# NOTE: These queries are slow to succeed because the default metrics scraping
# interval is 30 seconds.

# Until compute and storage are _entirely unified_, storage will not have
# logging dataflows, so the only way we can see that both processes are
# utilized is using `mz_source_statistics`. Timely only supports
# mapping workers 0,1 to process 0 and 2,3 to process 1, so we use a CTE
# here to distinguish between processes.
> WITH cte AS (
    SELECT
      CASE WHEN u.worker_id IN (0, 1) THEN 0 ELSE 1 END as process_id,
      SUM(u.messages_received) as messages_received
      FROM mz_sources s
      JOIN mz_internal.mz_source_statistics u on s.id = u.id
      WHERE s.name = 'many'
      GROUP BY u.worker_id
    )
    SELECT process_id, SUM(messages_received) > 0
    FROM cte
    GROUP BY process_id
0 true
1 true
