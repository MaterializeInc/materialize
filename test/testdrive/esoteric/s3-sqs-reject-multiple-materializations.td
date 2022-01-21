# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set buk=mz-sqs-double-mat-test

$ s3-create-bucket bucket=${buk}

$ s3-add-notifications bucket=${buk} queue=${buk} sqs-validation-timeout=5m


> CREATE SOURCE s3_double_mat
  FROM S3
  DISCOVER OBJECTS USING SQS NOTIFICATIONS 'testdrive-${buk}-${testdrive.seed}'
    WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT TEXT;

> CREATE MATERIALIZED VIEW s3_double_mat_initial_view AS
  SELECT * FROM s3_double_mat

> CREATE VIEW s3_double_mat_view_reuse AS
  SELECT text FROM s3_double_mat_initial_view

! CREATE MATERIALIZED VIEW s3_double_mat_error AS
  SELECT * FROM s3_double_mat
contains:Cannot re-materialize source s3_double_mat

# check that going through multiple unmaterialized views doesn't allow hiding from the error
> CREATE VIEW s3_double_mat_unmaterialized AS
  SELECT * FROM s3_double_mat

> CREATE VIEW s3_double_mat_unmaterialized_extra AS
  SELECT * FROM s3_double_mat_unmaterialized

! CREATE MATERIALIZED VIEW s3_double_mat_unmaterialized_final AS
  SELECT * FROM s3_double_mat_unmaterialized_extra
contains:Cannot re-materialize source s3_double_mat

# check that dropping and recreating a view succeeds

> DROP VIEW s3_double_mat_initial_view CASCADE

> CREATE MATERIALIZED VIEW s3_double_mat_initial_view_recreated AS
  SELECT * FROM s3_double_mat

# check that an already materialized item that goes through multiple
# unmaterialized views doesn't sneak past the check

> DROP VIEW s3_double_mat_initial_view_recreated CASCADE

> CREATE VIEW s3_double_mat_unmaterialized_2 AS
  SELECT * FROM s3_double_mat

> CREATE VIEW s3_double_mat_unmaterialized_extra_2 AS
  SELECT * FROM s3_double_mat_unmaterialized_2

> CREATE MATERIALIZED VIEW s3_double_mat_final_2 AS
  SELECT * FROM s3_double_mat_unmaterialized_extra_2

> CREATE MATERIALIZED VIEW s3_double_mat_final2_reuse AS
  SELECT text FROM s3_double_mat_final_2

! CREATE MATERIALIZED VIEW s3_double_mat_error AS
  SELECT * FROM s3_double_mat
contains:Cannot re-materialize source s3_double_mat

> CREATE VIEW s3_double_mat_unmaterialized_3 AS
  SELECT * FROM s3_double_mat

> CREATE VIEW s3_double_mat_unmaterialized_extra_3 AS
  SELECT * FROM s3_double_mat_unmaterialized_3

! CREATE MATERIALIZED VIEW s3_double_mat_error_3 AS
  SELECT * FROM s3_double_mat_unmaterialized_extra_3
contains:Cannot re-materialize source s3_double_mat

! CREATE INDEX s3_double_mat_custom_idx ON s3_double_mat (text)
contains:Cannot re-materialize source s3_double_mat

! CREATE DEFAULT INDEX ON s3_double_mat
contains:Cannot re-materialize source s3_double_mat

! CREATE SINK invalid_double_mat_sink FROM s3_double_mat
  INTO KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-kafka-sink-errors-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
contains:Cannot re-materialize source s3_double_mat
