# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests for AWS connections

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_aws_connection = true;
ALTER SYSTEM SET enable_connection_validation_syntax = true;

# Testing with assume role
> CREATE CONNECTION aws_assume_role
  TO AWS ( ASSUME ROLE ARN 'assume-role', ASSUME ROLE SESSION NAME 'session-name' );

$ set-from-sql var=conn_id
SELECT id FROM mz_connections WHERE name = 'aws_assume_role';

> SELECT * FROM mz_aws_connections WHERE id = '${conn_id}';
"${conn_id}" "<null>" "<null>" "<null>" "<null>" "assume-role" "session-name" "arn:aws:iam::123456789000:role/MaterializeConnection" "mz_eb5cb59b-e2fe-41f3-87ca-d2176a495345_${conn_id}" "{\"Statement\":[{\"Action\":\"sts:AssumeRole\",\"Condition\":{\"StringEquals\":{\"sts:ExternalId\":\"mz_eb5cb59b-e2fe-41f3-87ca-d2176a495345_${conn_id}\"}},\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"arn:aws:iam::123456789000:role/MaterializeConnection\"}}],\"Version\":\"2012-10-17\"}"

# Testing with access credentials
> CREATE SECRET s3_api_secret_key as 'secret_key';

> CREATE CONNECTION aws_credentials
  TO AWS (ACCESS KEY ID = 'access_key', SECRET ACCESS KEY = SECRET s3_api_secret_key);

$ set-from-sql var=conn_id
SELECT id FROM mz_connections WHERE name = 'aws_credentials';

> SELECT * FROM mz_aws_connections WHERE id = '${conn_id}';
"${conn_id}" "<null>" "<null>" access_key "<null>" "<null>" "<null>" "<null>" "<null>" "<null>"

# Testing with access_key as a secret
> CREATE SECRET s3_api_key as 'api_access_key';

> CREATE CONNECTION aws_credentials_with_secret
  TO AWS (ACCESS KEY ID = SECRET s3_api_key, SECRET ACCESS KEY = SECRET s3_api_secret_key);

$ set-from-sql var=conn_id
SELECT id FROM mz_connections WHERE name = 'aws_credentials_with_secret';

$ set-from-sql var=secret_id
SELECT id FROM mz_secrets WHERE name = 's3_api_key';

> SELECT * FROM mz_aws_connections WHERE id = '${conn_id}';
"${conn_id}" "<null>" "<null>" "<null>" "${secret_id}" "<null>" "<null>" "<null>" "<null>" "<null>"

# Testing invalid statements

! CREATE CONNECTION conn
  TO AWS ( ACCESS KEY ID = 'access_key' );
contains:Options [SECRET ACCESS KEY] are required for AWS Credentials connection

! CREATE CONNECTION conn
  TO AWS ( SECRET ACCESS KEY = SECRET s3_api_secret_key );
contains:Options [ACCESS KEY ID] are required for AWS Credentials connection

! CREATE CONNECTION conn
  TO AWS ( TOKEN = 'token');
contains:Options [ACCESS KEY ID, SECRET ACCESS KEY] are required for AWS Credentials connection

! CREATE CONNECTION conn
  TO AWS ( ASSUME ROLE SESSION NAME 'session-name' );
contains:Options [ASSUME ROLE ARN] are required for AWS AssumeRole connection

! CREATE CONNECTION conn
  TO AWS ( ACCESS KEY ID = 'access_key', SECRET ACCESS KEY = SECRET s3_api_secret_key, ASSUME ROLE ARN 'arn-name' );
contains:Only one of [ACCESS KEY ID and SECRET ACCESS KEY, or ASSUME ROLE ARN] can be provided for AWS connection

! CREATE CONNECTION conn
  TO AWS ( REGION 'us-east' );
contains:Options [ACCESS KEY ID and SECRET ACCESS KEY or ASSUME ROLE ARN] are required for AWS connection
