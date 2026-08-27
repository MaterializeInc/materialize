# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# An Iceberg sink must be able to write to storage using only the temporary,
# table-scoped credentials the REST catalog vends it.
#
# The Polaris catalog backing this test is created without client-visible S3
# credentials, so nothing in the config it returns lets Materialize reach MinIO
# on its own. The only usable credentials are the ones Polaris mints per table
# in response to the `X-Iceberg-Access-Delegation: vended-credentials` request
# that `ACCESS DELEGATION` below turns on. A sink that commits data here
# therefore exercises the vending path end to end. Were the request dropped, or
# the vended credentials not threaded into the sink's FileIO, the sink would
# fail to write rather than pass quietly.

# Delegation is opt-in. Only 'vended-credentials' is accepted.
! CREATE CONNECTION bad_delegation TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'http://polaris:8181/api/catalog',
    CREDENTIAL = 'root:root',
    WAREHOUSE = 'default_catalog',
    ACCESS DELEGATION = 'remote-signing'
  );
contains:invalid iceberg access delegation

> CREATE CONNECTION vended_polaris TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'http://polaris:8181/api/catalog',
    CREDENTIAL = 'root:root',
    WAREHOUSE = 'default_catalog',
    SCOPE = 'PRINCIPAL_ROLE:ALL',
    ACCESS DELEGATION = 'vended-credentials'
  );

> CREATE TABLE vended_src (a int, b text);

> INSERT INTO vended_src VALUES (1, 'one'), (2, 'two'), (3, 'three');

> CREATE SINK vended_sink
    FROM vended_src
    INTO ICEBERG CATALOG CONNECTION vended_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'vended_table'
    )
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');

# Iceberg sinks commit asynchronously; wait for at least one commit interval.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=10s

# A sink denied storage credentials stalls with an error instead of staying healthy.
> SELECT error IS NULL FROM mz_internal.mz_sink_statuses WHERE name = 'vended_sink'
true

# Read the table back with our own long-lived credentials, which are independent
# of whatever the catalog vended to Materialize.
$ duckdb-execute name=vended_iceberg
CREATE SECRET s3_secret_vended (TYPE S3, KEY_ID 'tduser', SECRET '${arg.s3-access-key}', ENDPOINT '${arg.aws-endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'minio');
SET unsafe_enable_version_guessing = true;

$ duckdb-query name=vended_iceberg
SELECT a, b FROM iceberg_scan('s3://test-bucket/default_namespace/vended_table') ORDER BY a
1 one
2 two
3 three
