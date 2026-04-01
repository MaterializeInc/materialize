# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that CREATE SINK INTO ICEBERG rejects unsupported key column types.
# Iceberg equality deletes require primitive, non-floating-point key columns.

> CREATE SECRET IF NOT EXISTS access_key_secret AS '${arg.s3-access-key}'

> CREATE CONNECTION IF NOT EXISTS aws_conn TO AWS (
    ACCESS KEY ID = 'tduser',
    SECRET ACCESS KEY = SECRET access_key_secret,
    ENDPOINT = '${arg.aws-endpoint}',
    REGION = 'us-east-1'
  );

> CREATE CONNECTION IF NOT EXISTS polaris TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'http://polaris:8181/api/catalog',
    CREDENTIAL = 'root:root',
    WAREHOUSE = 'default_catalog',
    SCOPE = 'PRINCIPAL_ROLE:ALL'
  );

# Float key
> CREATE TABLE key_float (k float, v text);

! CREATE SINK key_float_sink
    FROM key_float
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'key_float_table'
    )
    USING AWS CONNECTION aws_conn
    KEY (k) NOT ENFORCED
    MODE UPSERT
    WITH (COMMIT INTERVAL '1s');
contains:cannot be used as an Iceberg equality delete key

# Double key
> CREATE TABLE key_double (k double, v text);

! CREATE SINK key_double_sink
    FROM key_double
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'key_double_table'
    )
    USING AWS CONNECTION aws_conn
    KEY (k) NOT ENFORCED
    MODE UPSERT
    WITH (COMMIT INTERVAL '1s');
contains:cannot be used as an Iceberg equality delete key

# Map key
> CREATE TABLE key_map (k map[text=>text], v text);

! CREATE SINK key_map_sink
    FROM key_map
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'key_map_table'
    )
    USING AWS CONNECTION aws_conn
    KEY (k) NOT ENFORCED
    MODE UPSERT
    WITH (COMMIT INTERVAL '1s');
contains:cannot be used as an Iceberg equality delete key

# List key
> CREATE TABLE key_list (k int list, v text);

! CREATE SINK key_list_sink
    FROM key_list
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'key_list_table'
    )
    USING AWS CONNECTION aws_conn
    KEY (k) NOT ENFORCED
    MODE UPSERT
    WITH (COMMIT INTERVAL '1s');
contains:cannot be used as an Iceberg equality delete key

# Non-key float column is fine
> CREATE TABLE val_float (k int, v float);

# This should succeed (only the KEY column matters, not all columns)
> CREATE SINK val_float_sink
    FROM val_float
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'val_float_table'
    )
    USING AWS CONNECTION aws_conn
    KEY (k) NOT ENFORCED
    MODE UPSERT
    WITH (COMMIT INTERVAL '1s');

> DROP SINK val_float_sink;
> DROP TABLE val_float;
> DROP TABLE key_float;
> DROP TABLE key_double;
> DROP TABLE key_map;
> DROP TABLE key_list;
