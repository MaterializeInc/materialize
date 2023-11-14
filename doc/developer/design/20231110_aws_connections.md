# AWS Connection

## The Problem

Github issue:  [Issue 23055](https://github.com/MaterializeInc/materialize/issues/23055)

We plan to build an AWS S3 sink, in which we write to an S3 bucket under the end user's control. We'll need to integrate with AWS IAM so that our users can grant their Materialize region the ability to write to their desired S3 bucket.

Integration with AWS IAM would also be useful for facilitating IAM authentication with the following existing source types:

- PostgreSQL on AWS RDS
- Kafka on AWS MSK

While we support AWS RDS/MSK today via traditional username/password authentication, many users prefer to use AWS IAM with these services, as it eliminates the need to manage additional user accounts/passwords.

## Success Criteria
- Be able to create an AWS connection using either AWS credentials or by using the [AssumeRole API](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html)

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
TODO: Check with the cloud team on the external ID generation.

## Open questions

#### What should be the principal?
For users to be able to set up the trust policy on their end, we need to provide them
with the principal and an External ID. The principal can be our AWS account id or a user/role
we specifically create for this.

TODO: Check what we do with existing aws private links where a similar principal is needed.

#### What should be the behaviour for `VALIDATE CONNECTION` of an AWS connection?
TODO: Is there an API in AWS SDK to validate if AssumeRole is set correctly or maybe we can
try to fetch temporary credentials.
Note: An AWS connection can be potentially re-used across multiple services like S3 or RDS.
Having permission to an S3 bucket might not mean that RDS access is set up correctly.

Potentially later we can extend `VALIDATE CONNECTION` sql to optionally specify an S3 or a database url, like,
`VALIDATE CONNECTION aws_conn WITH (S3 PATH = 's3://prefix')`

Alternatively, we could have a higher level S3 connection using this AWS connection, something like
`CREATE CONNECTION s3_conn TO S3 ( PREFIX = 'url') USING AWS CONNECTION aws_conn`. Validating s3_conn
could verify if we are able to list the prefix, create and remove a file there.

#### How should we test?
TODO: Check with the cloud team if we can set up cloudtests.
