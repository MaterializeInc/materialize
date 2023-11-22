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

#### CREATE CONNECTION
We already have some [existing code](https://github.com/MaterializeInc/materialize/blob/v0.77.1/src/storage-types/src/connections/aws.rs), which can be re-used to
handle the AssumeRole scenario.

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

When creating the connection, users should specify `WITH (VALIDATE = false)` in the sql.
This is because we won't be able to validate the connection unless the trust policy has
been added on the user's end.

#### External ID and principal
We already have external ID prefix provided in the catalog state.
The connection ID will be appended to the external ID prefix to get the complete
External ID and then stored in the new `mz_aws_connections` table for that connection ID.

For principal, we'll create one global role named something like
`arn:aws:iam::<account_id>:role/MaterializeCloudConnectionAssumeRole`
which all environmentd/clusterd-s can assume. This will add some good restrictions
on who could assume the customers role and also allow for migration between environment clusters.

Note: There's a hard limit of 1 hour on the session duration when [Role chaining in AWS](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_terms-and-concepts.html).
This can be revisited for workarounds if we hit this issue.

Other alternative solutions were:
1. Using our root account `arn:aws:iam::<account_id>:root`. This would work but this is not
secure enough since any user/role in this account can assume the customer's role.
2. An IAM role `arn:aws:iam::<account_id>:role/<id>` we create for each customer environment. This
will not scale, there's a maximum limit of [5000 roles](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_iam-quotas.html#reference_iam-quotas-entities)
we can create per account.
3. An IAM user `arn:aws:iam::<account_id>:user/<id>` created for the customer's environment similar
to [Snowflake](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration#step-4-retrieve-the-aws-iam-user-for-your-snowflake-account).
This could work if we keep them in a secret and rotate regularly, this would give
each customer a unique principal. But we'd have to rotate very frequently and we may also
need to coordinate that role across all users of the credentials which seems like a lot of overhead.
There's also a limit of 5000 to [number of users in an account in a region](https://docs.aws.amazon.com/general/latest/gr/iam-service.html),
so this will not scale as well.

#### VALIDATE CONNECTION
We can make use of the [GetCallerIdentity AWS API](https://docs.aws.amazon.com/STS/latest/APIReference/API_GetCallerIdentity.html)
to check if the credentials are valid. We should make the call with the given External ID and
without to make sure that the user has a secure setup. If we are able to access without the External
ID, we should treat that as an error.

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

#### Possibly extend `VALIDATE CONNECTION` of an AWS connection to be comprehensive.
An AWS connection can be potentially re-used across multiple services like S3 or RDS.
Having permission to an S3 bucket might not mean that RDS access is set up correctly. So
a validate connection will not be comprehensive and we'll probably do some additional check
with the intended resources when we actually make use of this connection.

To properly validate the connection we should ask the user to actually use the connection,
something like `COPY (SELECT 1) TO S3 USING aws_conn`.

We could also extend `VALIDATE CONNECTION` sql to optionally specify an S3 or a database url,
like, `VALIDATE CONNECTION aws_conn WITH (S3 PATH = 's3://prefix')` so the user can
validate if the policies are set up correctly on their end in AWS. One reason this might be
difficult though, because the only permissions we should require is `ListBucket` and `PutObject`. And to test if we can write something to the prefix without `RemoveObject`, we end up with leaked
objects which we can't clean.

Alternatively, we could have a higher level S3 connection using this AWS connection,something
like `CREATE CONNECTION s3_conn TO S3 ( PREFIX = 'url') USING AWS CONNECTION aws_conn`.
Validating s3_conn could verify if we are able to list the prefix, create and
remove a file there. This seems like an overkill though and the approach above would probably be better.

We will revisit this when designing the `COPY ... TO S3` feature.
