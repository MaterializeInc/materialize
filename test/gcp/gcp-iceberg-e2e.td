# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE SECRET gcpsa AS decode('${arg.gcp-sa-json-b64}', 'base64');

> CREATE CONNECTION gcpconn TO GCP (SERVICE ACCOUNT KEY = SECRET gcpsa);

> CREATE CONNECTION biglake TO ICEBERG CATALOG (
    CATALOG TYPE = 'rest',
    URL = 'https://biglake.googleapis.com/iceberg/v1/restcatalog',
    GCP CONNECTION = gcpconn,
    WAREHOUSE = 'gs://${arg.gcs-bucket}'
  );

> create table foo(a int, b text);

> insert into foo values (1, 'one'), (2, 'two'), (3, 'three');

> CREATE SINK demo
    FROM foo
    INTO ICEBERG CATALOG CONNECTION biglake (
        NAMESPACE '${arg.namespace}',
        TABLE '${arg.table}'
    )
    KEY (a) NOT ENFORCED
    MODE UPSERT
    WITH (COMMIT INTERVAL '1s');

# Iceberg sinks commit data asynchronously. We leave the sink running and let
# mzcompose.py poll BigLake until the commit lands or times out.
