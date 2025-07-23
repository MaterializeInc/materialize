# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests for AWS connections.

> SET transaction_isolation = 'strict serializable'

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_connection_validation_syntax = true;

# Test assume role connections.

> CREATE CONNECTION aws_assume_role
  TO AWS (ASSUME ROLE ARN 'assume-role', ASSUME ROLE SESSION NAME 'session-name');

$ set-from-sql var=conn-id
SELECT id FROM mz_connections WHERE name = 'aws_assume_role';

> SELECT * FROM mz_internal.mz_aws_connections WHERE id = '${conn-id}';
id           endpoint  region  access_key_id  access_key_id_secret_id  secret_access_key_secret_id  session_token  session_token_secret_id  assume_role_arn  assume_role_session_name  principal                                   external_id                                          example_trust_policy
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
"${conn-id}" <null>    <null>  <null>         <null>                   <null>                       <null>         <null>                   assume-role      session-name              arn:aws:iam::123456789000:role/MaterializeConnection "mz_eb5cb59b-e2fe-41f3-87ca-d2176a495345_${conn-id}" "{\"Statement\":[{\"Action\":\"sts:AssumeRole\",\"Condition\":{\"StringEquals\":{\"sts:ExternalId\":\"mz_eb5cb59b-e2fe-41f3-87ca-d2176a495345_${conn-id}\"}},\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"arn:aws:iam::123456789000:role/MaterializeConnection\"}}],\"Version\":\"2012-10-17\"}"

# Test access credentials connections.

> CREATE SECRET aws_secret_access_key as '...';

> CREATE CONNECTION aws_credentials
  TO AWS (ACCESS KEY ID = 'access_key', SECRET ACCESS KEY = SECRET aws_secret_access_key);

$ set-from-sql var=conn-id
SELECT id FROM mz_connections WHERE name = 'aws_credentials';

$ set-from-sql var=secret-key-secret-id
SELECT id FROM mz_secrets WHERE name = 'aws_secret_access_key';

> SELECT * FROM mz_internal.mz_aws_connections WHERE id = '${conn-id}';
id           endpoint  region  access_key_id  access_key_id_secret_id  secret_access_key_secret_id  session_token  session_token_secret_id  assume_role_arn  assume_role_session_name  principal  external_id  example_trust_policy
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
\${conn-id}  <null>   <null>   access_key     <null>                   ${secret-key-secret-id}      <null>         <null>                   <null>           <null>                    <null>     <null>       <null>

# Test access credentials connections where the access key ID is also a secret.

> CREATE SECRET aws_access_key_id as '...';

> CREATE CONNECTION aws_credentials_with_secret
  TO AWS (ACCESS KEY ID = SECRET aws_access_key_id, SECRET ACCESS KEY = SECRET aws_secret_access_key);

$ set-from-sql var=conn-id
SELECT id FROM mz_connections WHERE name = 'aws_credentials_with_secret';

$ set-from-sql var=access-key-secret-id
SELECT id FROM mz_secrets WHERE name = 'aws_access_key_id';

> SELECT * FROM mz_internal.mz_aws_connections WHERE id = '${conn-id}';
id           endpoint  region  access_key_id  access_key_id_secret_id  secret_access_key_secret_id  session_token  session_token_secret_id  assume_role_arn  assume_role_session_name  principal  external_id  example_trust_policy
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
\${conn-id}  <null>   <null>   <null>         ${access-key-secret-id}  ${secret-key-secret-id}      <null>         <null>                   <null>           <null>                    <null>     <null>       <null>

# Tests for validating connections are in test/aws/.

# Test invalid statements.

! CREATE CONNECTION conn
  TO AWS (ACCESS KEY ID = 'access_key');
contains:must specify both ACCESS KEY ID and SECRET ACCESS KEY with optional SESSION TOKEN

! CREATE CONNECTION conn
  TO AWS (SECRET ACCESS KEY = SECRET aws_secret_access_key);
contains:must specify both ACCESS KEY ID and SECRET ACCESS KEY with optional SESSION TOKEN

! CREATE CONNECTION conn
  TO AWS (SESSION TOKEN = 'token');
contains:must specify both ACCESS KEY ID and SECRET ACCESS KEY with optional SESSION TOKEN

! CREATE CONNECTION conn
  TO AWS (ASSUME ROLE SESSION NAME 'session-name');
contains:must specify ASSUME ROLE ARN with optional ASSUME ROLE SESSION NAME

! CREATE CONNECTION conn
  TO AWS (ACCESS KEY ID = 'access_key', SECRET ACCESS KEY = SECRET aws_secret_access_key, ASSUME ROLE ARN 'arn-name');
contains:cannot specify both ACCESS KEY ID and ASSUME ROLE ARN

! CREATE CONNECTION conn
  TO AWS (REGION 'us-east');
contains:must specify either ASSUME ROLE ARN or ACCESS KEY ID and SECRET ACCESS KEY

# Tests for invalid IAM configurations are in test/aws/.
