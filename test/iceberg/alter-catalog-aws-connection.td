# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# ALTER CONNECTION on the AWS connection behind an S3 Tables catalog must not
# crash environmentd while an Iceberg sink depends on that catalog. The catalog
# connection inlines the AWS connection, so the alter changes the sink's catalog
# connection too.
#
# The sink never reaches a catalog. The S3 Tables client fetches its config
# lazily, so CREATE SINK succeeds without one, and the sink only has to exist
# for the alter to reach it.

> CREATE SECRET s3tables_secret AS 'secret'

> CREATE CONNECTION s3tables_aws TO AWS (
    ACCESS KEY ID = 'old-key',
    SECRET ACCESS KEY = SECRET s3tables_secret,
    REGION = 'us-east-1'
  );

> CREATE CONNECTION s3tables_catalog TO ICEBERG CATALOG (
    CATALOG TYPE = 's3tablesrest',
    URL = 'http://s3tables.invalid/iceberg',
    WAREHOUSE = 'arn:aws:s3tables:us-east-1:000000000000:bucket/test',
    AWS CONNECTION = s3tables_aws
  ) WITH (VALIDATE = false);

> CREATE TABLE t (a int, b text);

> CREATE SINK s3tables_sink
    FROM t
    INTO ICEBERG CATALOG CONNECTION s3tables_catalog (
        NAMESPACE 'default_namespace',
        TABLE 's3tables_table'
    )
    USING AWS CONNECTION s3tables_aws
    KEY (a) NOT ENFORCED
    MODE UPSERT
    WITH (COMMIT INTERVAL '1s');

# Setting the key to its current value leaves the catalog connection unchanged.
> ALTER CONNECTION s3tables_aws SET (ACCESS KEY ID = 'old-key')

# Rotating the access key changes the inlined catalog connection.
> ALTER CONNECTION s3tables_aws SET (ACCESS KEY ID = 'new-key')

> SELECT create_sql LIKE '%ACCESS KEY ID = ''new-key''%' FROM (SHOW CREATE CONNECTION s3tables_aws)
true

> SELECT name FROM mz_sinks WHERE name = 's3tables_sink'
s3tables_sink
