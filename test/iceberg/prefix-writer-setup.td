# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE SECRET access_key_secret AS '${arg.s3-access-key}'

> CREATE CONNECTION aws_conn TO AWS (
    ACCESS KEY ID = 'tduser',
    SECRET ACCESS KEY = SECRET access_key_secret,
    ENDPOINT = '${arg.aws-endpoint}',
    REGION = 'us-east-1'
  );

> CREATE CONNECTION polaris TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'http://polaris:8181/api/catalog',
    CREDENTIAL = 'root:root',
    WAREHOUSE = 'default_catalog',
    SCOPE = 'PRINCIPAL_ROLE:ALL'
  );

> CREATE TABLE prefix_src (id INT, val TEXT);

> INSERT INTO prefix_src VALUES (1, 'a'), (2, 'b'), (3, 'c');

# The long commit interval makes the sink's pending batch wide, so a forged
# frontier one second past the last committed one lands strictly inside it.
> CREATE SINK prefix_sink
    FROM prefix_src
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'prefix_table'
    )
    USING AWS CONNECTION aws_conn
    KEY (id) NOT ENFORCED
    MODE UPSERT
    WITH (COMMIT INTERVAL '30s');
