# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# STORAGE PROVIDER names the object store holding a REST catalog's data files.
# The catalog protocol does not report this: it hands back locations and
# credentials and expects the client to already know how to reach that store.
#
# Everything asserted here is decided at plan time, so skip the
# connect-on-create validation and point at hosts that do not exist.
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_default_connection_validation = false

> CREATE SECRET sp_cred AS 'client-id:client-secret'

# Omitting the option leaves the default of S3 in place. It is not echoed back,
# so an unset provider stays unset in the catalog rather than being normalized
# to an explicit 's3'.
> CREATE CONNECTION sp_default TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'https://example.invalid/api/catalog',
    CREDENTIAL = SECRET sp_cred,
    WAREHOUSE = 'wh'
  );

> SHOW CREATE CONNECTION sp_default
name create_sql
---------------------------------
materialize.public.sp_default "CREATE CONNECTION materialize.public.sp_default\nTO ICEBERG CATALOG\n    (\n        CREDENTIAL = SECRET materialize.public.sp_cred,\n        CATALOG TYPE = 'REST',\n        URL = 'https://example.invalid/api/catalog',\n        WAREHOUSE = 'wh'\n    );"

# Each accepted value round-trips through the catalog.
> CREATE CONNECTION sp_s3 TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'https://example.invalid/api/catalog',
    CREDENTIAL = SECRET sp_cred,
    STORAGE PROVIDER = 's3',
    WAREHOUSE = 'wh'
  );

> CREATE CONNECTION sp_gcs TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'https://example.invalid/api/catalog',
    CREDENTIAL = SECRET sp_cred,
    STORAGE PROVIDER = 'gcs',
    WAREHOUSE = 'wh'
  );

> CREATE CONNECTION sp_adls TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'https://example.invalid/api/catalog',
    CREDENTIAL = SECRET sp_cred,
    STORAGE PROVIDER = 'adls',
    WAREHOUSE = 'wh'
  );

> SELECT name FROM mz_connections WHERE name LIKE 'sp\_%' ORDER BY name
sp_adls
sp_default
sp_gcs
sp_s3

# The value is case-insensitive, like CATALOG TYPE.
> CREATE CONNECTION sp_upper TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'https://example.invalid/api/catalog',
    CREDENTIAL = SECRET sp_cred,
    STORAGE PROVIDER = 'GCS',
    WAREHOUSE = 'wh'
  );

! CREATE CONNECTION sp_bad TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'https://example.invalid/api/catalog',
    CREDENTIAL = SECRET sp_cred,
    STORAGE PROVIDER = 'azure',
    WAREHOUSE = 'wh'
  );
contains:invalid iceberg storage provider, expected 's3', 'gcs', or 'adls'

# S3 Tables is always backed by S3, so the option is meaningless there rather
# than merely redundant.
! CREATE CONNECTION sp_on_s3tables TO ICEBERG CATALOG (
    CATALOG TYPE = 's3tablesrest',
    URL = 'https://example.invalid/api/catalog',
    STORAGE PROVIDER = 's3',
    WAREHOUSE = 'wh'
  );
contains:s3tablesrest connections do not support STORAGE PROVIDER

> DROP CONNECTION sp_upper
> DROP CONNECTION sp_adls
> DROP CONNECTION sp_gcs
> DROP CONNECTION sp_s3
> DROP CONNECTION sp_default
> DROP SECRET sp_cred
