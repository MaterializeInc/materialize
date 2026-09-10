# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Create an Iceberg sink into a real Databricks Unity Catalog. Everything the
# catalog does with it is asserted from mzcompose.py, which can read the Iceberg
# REST API directly; this file only stands the pipeline up.

# The credential is `client_id:client_secret`, which is how an ICEBERG CATALOG
# connection expects an OAuth client-credentials pair.
> CREATE SECRET databricks_oauth AS '${arg.databricks-credential}'

# Unity Catalog serves its token endpoint on a path unrelated to the catalog
# URL, so OAUTH2 SERVER URL is required: the specification-derived default of
# <url>/v1/oauth/tokens does not exist here. ACCESS DELEGATION is likewise not
# optional, because Materialize holds no credentials for the storage Unity
# Catalog manages.
> CREATE CONNECTION databricks_uc TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = '${arg.workspace-url}/api/2.1/unity-catalog/iceberg-rest',
    CREDENTIAL = SECRET databricks_oauth,
    OAUTH2 SERVER URL = '${arg.workspace-url}/oidc/v1/token',
    SCOPE = 'all-apis',
    WAREHOUSE = '${arg.catalog}',
    ACCESS DELEGATION = 'vended-credentials'
  )

> CREATE TABLE src (n bigint, payload text)

> INSERT INTO src SELECT n, 'row_' || n::text FROM generate_series(1, ${arg.rows}) AS n

> CREATE SINK databricks_sink
    FROM src
    INTO ICEBERG CATALOG CONNECTION databricks_uc (
        NAMESPACE '${arg.namespace}',
        TABLE '${arg.table}'
    )
    MODE APPEND
    WITH (COMMIT INTERVAL '${arg.commit-interval}')

# A sink that cannot reach the catalog or cannot get storage credentials stalls
# with an error rather than staying healthy, so this catches a broken connection
# before the workflow settles in to wait an hour.
> SELECT error IS NULL FROM mz_internal.mz_sink_statuses WHERE name = 'databricks_sink'
true
