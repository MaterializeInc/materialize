# AWS Connection

## The Problem

Github issue:  [Issue 23055](https://github.com/MaterializeInc/materialize/issues/23055)

We plan to build an AWS S3 sink, in which we write to an S3 bucket under the end user's
control. We'll need to integrate with AWS IAM so that our users can grant their Materialize
region the ability to write to their desired S3 bucket.

Integration with AWS IAM would also be useful for facilitating IAM authentication with the
following existing source types:

- PostgreSQL on AWS RDS
- Kafka on AWS MSK

While we support AWS RDS/MSK today via traditional username/password authentication, many
users prefer to use AWS IAM with these services, as it eliminates the need to manage
additional user accounts/passwords.

## Success Criteria
- Be able to create an AWS connection using either AWS credentials or by using the [AssumeRole API](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html).
- Be able to check that the AWS connection is valid or not using `VALIDATE CONNECTION`.

## Solution Proposal
Allow users to create an AWS CONNECTION with AWS Credentials or AWS IAM AssumeRole.

### AWS Credentials
Creating an AWS connection with credentials should be quite straightforward. The SQL would look like,
```
CREATE SECRET aws_secret_key = 'secret';

CREATE CONNECTION aws_conn TO AWS (
  ACCESS KEY ID = 'access_key',
  SECRET ACCESS KEY = SECRET aws_secret_key
);

```
The `aws_conn` is now available to be used in supported queries.

### AWS IAM AssumeRole
Users will need to first create a role on their end, say, `MaterializeConn` and note down it's arn, e.g: `arn:aws:iam::001234567890:role/MaterializeConn`.

User then creates a connection in Materialize with that arn.
```
CREATE CONNECTION aws_conn TO AWS (
  ASSUME ROLE ARN = 'arn:aws:iam::001234567890:role/MaterializeConn'
);
```
On Materialize's end we generate an External ID and create an entry in a new `mz_aws_connections` table.
Users should be able to get the external_id and the principal by querying this table.
```
SELECT principal, external_id
FROM mz_aws_connections aws
JOIN mz_connections c ON aws.id = c.id
WHERE c.name = 'aws_conn';
```

Users should then be able to add a trust policy in their AWS account to give access to Materialize.
```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "AWS": "<principal>"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<external_id>"
        }
      }
    }
  ]
}
```

## Implementation
We already have some [existing code](https://github.com/MaterializeInc/materialize/blob/v0.77.1/src/storage-types/src/connections/aws.rs), which can be re-used to
handle the AssumeRole scenario as well.

In the parser we already support `ROLE ARN` option which can be renamed to `ASSUME ROLE ARN`
to better reflect the usage. We will need to add an `ASSUME ROLE SESSION NAME` option as well.

The AWS Connection create statement with all the options will now look like:
```sql
CREATE CONNECTION <connector_name> TO AWS (
  ACCESS KEY ID [[=] <value>], -- existing
  SECRET ACCESS KEY [[=] SECRET <value>], -- existing
  TOKEN [[=] SECRET <value>], -- existing
  ASSUME ROLE ARN [[=] <value>], -- rename existing from ROLE ARN
  ASSUME ROLE SESSION NAME [[=] <value>], -- new option
  ENDPOINT [[=] <value>], -- existing
  REGION [[=] <value>] -- existing
);
```

Users should be able to provide either the `ACCESS KEY` options or the `ASSUME ROLE` options,
but not both.

#### External ID and principal
We already have external ID prefix provided in the catalog state.
The connection ID will be appended to the external ID prefix to get the complete
External ID and then stored in the new `mz_aws_connections` table for that connection ID.
For principal, refer to the [open question below](#what-should-be-the-principal-for-this-connection).

#### `VALIDATE CONNECTION`
We can make use of the [GetCallerIdentity AWS API](https://docs.aws.amazon.com/STS/latest/APIReference/API_GetCallerIdentity.html)
to check if the credentials are valid.

Optionally, with `ASSUME ROLE ARN` we can also try to get temporary credentials.

## Rollout and Testing
We should put the AWS connection behind a feature flag.

#### Testing During development
Write cloudtests to test out the different AWS Connections. For the AssumeRole AWS
Connection we should test that the expected principal and external_id is populated
in the `mz_aws_connections` table.

#### Testing after code is merged
Switch on the feature flag for the staging environment and create an AWS Connection
in staging followed by `VALIDATE CONNECTION`.

## Future work

#### Extend `VALIDATE CONNECTION` of an AWS connection to be comprehensive.
An AWS connection can be potentially re-used across multiple services like S3 or RDS.
Having permission to an S3 bucket might not mean that RDS access is set up correctly. So
a validate connection will not be comprehensive and we'll probably do some additional check
with the intended resources when we actually make use of this connection.

Hence, potentially later we can extend `VALIDATE CONNECTION` sql to optionally specify an S3 or a database url,
like, `VALIDATE CONNECTION aws_conn WITH (S3 PATH = 's3://prefix')` so the user can correctly
validate if the policies are set up correctly on their end in AWS.

Alternatively, we could have a higher level S3 connection using this AWS connection,something
like `CREATE CONNECTION s3_conn TO S3 ( PREFIX = 'url') USING AWS CONNECTION aws_conn`.
Validating s3_conn could verify if we are able to list the prefix, create and
remove a file there. This seems like an overkill though and a `VALIDATE CONNECTION ... WITH`
mentioned above would probably be better.

## Open Questions
#### What should be the `principal` for this connection?
For an AssumeRole to work, we need to provide the principal to the user using which we'll try to assume the role.
I had initially assumed that the principal we generate for AWS Privatelink might work, but that does not seem to be the case.
Among many things, the principal can be
1. Our account ID, corresponding to `arn:aws:iam::<account_id>:root`
2. An IAM user `arn:aws:iam::<account_id>:user/<id>` we create for each user environment
3. An IAM role `arn:aws:iam::<account_id>:role/<id>` we create for each user environment

We could simplify and directly use the root account ID (Option 1). But this seems insecure that any materialize role/user
belonging to this account would be able to assume this user's role.

Option 3 will not scale, there's a maximum limit of [5000 roles](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_iam-quotas.html#reference_iam-quotas-entities) we can create per account.

Snowflake uses Option 2, they create [one IAM user per user](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration#step-4-retrieve-the-aws-iam-user-for-your-snowflake-account).
If we want to do this, this will require additional work on the cloud's side to create this user and
also how would access and auth work in environmentd/clusterd.
