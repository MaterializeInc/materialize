---
title: "Snowflake"
description: "How to export results from Materialize to Snowflake."
menu:
  main:
    parent: sink
    name: "Snowflake"
    weight: 45
---

[//]: # "TODO(morsapaes) For Kafka users, it's possible to sink data to
Snowflake continuously using the Snowflake connector for Kafka. We should also
document that approach."

{{< public-preview />}}

This guide walks you through the steps required to bulk-export results from
Materialize to Snowflake using Amazon S3 as the intermediate object store.

## Before you begin

- Ensure you have access to an AWS account, and permissions to create and manage
  IAM policies and roles. If you're not an account administrator, you will need
  support from one!

- Ensure you have access to a Snowflake account, and are able to connect as a
  user with either the [`ACCOUNTADMIN` role](https://docs.snowflake.com/en/user-guide/security-access-control-considerations#using-the-accountadmin-role),
  or a role with the [global `CREATE INTEGRATION` privilege](https://docs.snowflake.com/en/user-guide/security-access-control-privileges#global-privileges-account-level-privileges).

## Step 1. Set up bulk exports to Amazon S3

Follow the [Amazon S3 integration guide](/serve-results/s3/) to set up an Amazon
S3 bucket that Materialize securely writes data into. This will be your
starting point for bulk-loading Materialize data into Snowflake.

## Step 2. Configure a Snowflake storage integration

### Create an IAM policy

To bulk-load data from an S3 bucket into Snowflake, you must create a new
[IAM policy](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html)
that specifies what actions can be performed on the bucket by the Snowflake
importer role. For Snowflake to be able to read data from the bucket, the IAM
policy must allow the following actions:

Action type  | Action name                                                                            | Action description
-------------|----------------------------------------------------------------------------------------|---------------
Write        | [`s3:GetBucketLocation`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html) | Grants permission to return the region the bucket is hosted in.
Read         | [`s3:GetObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html) | Grants permission to retrieve objects from a bucket.
Read        | [`s3:GetObjectVersion`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)| Grants permission to retrieve a specific version of an object from a bucket.
List        | [`s3:ListBucket`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)| Grants permission to list some or all of the objects in a bucket.
Write        | [`s3:DeleteObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html) | (Optional) Grants permission to remove an object from a bucket.
Write        | [`s3:DeleteObjectVersion`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html) | (Optional) Grants permission to remove a specific version of an object from a bucket.
Write        | [`s3:PutObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html) | (Optional) Grants permission to add an object to a bucket.

To create a new IAM policy:

1. Navigate to **AWS Services**, then **AWS IAM**.

1. In the **IAM Dashboard**, click **Policies**, then **Create policy**.

1. For **Policy editor**, choose **JSON**.

1. Copy and paste the policy below into the editor, replacing `<bucket>` with
   the bucket name and `<prefix>` with the folder path prefix.

   ```json
   {
      "Version": "2012-10-17",
      "Statement": [
         {
            "Effect": "Allow",
            "Action": [
               "s3:PutObject",
               "s3:GetObject",
               "s3:GetObjectVersion",
               "s3:DeleteObject",
               "s3:DeleteObjectVersion"
            ],
            "Resource": "arn:aws:s3:::<bucket>/<prefix>/*"
         },
         {
            "Effect": "Allow",
            "Action": [
               "s3:ListBucket",
               "s3:GetBucketLocation"
            ],
            "Resource": "arn:aws:s3:::<bucket>",
            "Condition": {
               "StringLike": {
                  "s3:prefix": [
                     "<prefix>/*"
                  ]
               }
            }
         }
      ]
   }
   ```

1. Click **Next**.

1. Enter a name for the policy, and click **Create policy**.

### Create an IAM role

Next, you must attach the policy you just created to a Snowflake-specific
[IAM role](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html).

1. Navigate to **AWS Services**, then **AWS IAM**.

1. In the **IAM Dashboard**, click **Roles**, then **Create role**.

1. In **Trusted entity type**, select **Account ID**, then **This account**.
   Later, you'll update it with the unique identifier for your Snowflake account.

1. Check the **Require external ID** box. Enter a placeholder **External ID**
   (e.g. 0000). Later, you'll update it with the unique external ID for your
   Snowflake storage integration.

1. Click **Next**.

1. In **Add permissions**, select the IAM policy you created in [Create an IAM policy](#create-an-iam-policy),
   and click **Next**.

1. Enter a name for the role, and click **Create role**.

1. Click **View role** to see the role summary page, and note down the
   role **ARN**. You will need it in the next step to create a Snowflake storage
   integration.

### Create a Snowflake storage integration

{{< note >}}
Only users with either the [`ACCOUNTADMIN` role](https://docs.snowflake.com/en/user-guide/security-access-control-considerations#using-the-accountadmin-role),
or a role with the [global `CREATE INTEGRATION` privilege](https://docs.snowflake.com/en/user-guide/security-access-control-privileges#global-privileges-account-level-privileges)
can execute this step.
{{< /note >}}

1. In [Snowsight](https://app.snowflake.com/), or your preferred SQL client
connected to Snowflake, create a [storage integration](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration),
replacing `<role>` with the name of the role you created in the previous step:

   ```sql
   CREATE STORAGE INTEGRATION S3_int
     TYPE = EXTERNAL_STAGE
     STORAGE_PROVIDER = 'S3'
     ENABLED = TRUE
     STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::001234567890:role/<role>'
     STORAGE_ALLOWED_LOCATIONS = ('*');
   ```

1. Retrieve the IAM principal for your Snowflake account using the
   [`DESC INTEGRATION`](https://docs.snowflake.com/en/sql-reference/sql/desc-integration)
   command:

   ```sql
   DESC INTEGRATION s3_int;
   ```

   Note down the values for the `STORAGE_AWS_IAM_USER_ARN` and
   `STORAGE_AWS_EXTERNAL_ID` properties. You will need them in the next step to
   update the Snowflake trust policy attached to your S3 bucket.

### Update the IAM policy

1. In your AWS account, find the IAM role you created in [Create an IAM role](#create-an-iam-role)
   and, under **Trust relationships**, click **Edit trust policy**. Use the values
   for the `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` properties
   from the previous step to update the trust policy's `Principal` and
   `ExternalId`, then click **Update policy**.

## Step 3. Create a Snowflake external stage

Back in Snowflake, create an [external stage](https://docs.snowflake.com/en/user-guide/data-load-S3-create-stage#external-stages) that uses the storage integration above and references your S3 bucket.

```sql
CREATE STAGE s3_stage
  STORAGE_INTEGRATION = s3_int
  URL = 's3://<bucket>/<prefix>/';
```

{{< note >}}
To create a stage that uses a storage integration, the active user must have a
role with the [`CREATE STAGE` privilege](https://docs.snowflake.com/en/sql-reference/sql/create-stage)
for the active schema, as well as the [`USAGE` privilege](https://docs.snowflake.com/en/sql-reference/sql/grant-privilege#syntax)
on the relevant storage integration.
{{< /note >}}

## Step 4. Import data into Snowflake

To import the data stored in S3 into Snowflake, you can then create a table and
use the [`COPY INTO`](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table)
command to load it from the external stage.

{{< tabs >}}
{{< tab "Parquet">}}

Create a table with a single column of type [`VARIANT`](https://docs.snowflake.com/en/sql-reference/data-types-semistructured#variant):

```sql
CREATE TABLE s3_table_parquet (
    col VARIANT
);
```

Use `COPY INTO` to load the data into the table:

```sql
COPY INTO s3_table_parquet
  FROM @S3_stage
  FILE_FORMAT = (TYPE = 'PARQUET');
```

For more details on importing Parquet files staged in S3 into Snowflake, check the
[Snowflake documentation](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table#type-parquet).

{{< /tab >}}

{{< tab "CSV">}}

Create a table with the same number of columns as the number of delimited
columns in the input data file:

```sql
CREATE TABLE s3_table_csv (
    col_1 INT,
    col_2 TEXT,
    col_3 TIMESTAMP
);
```

Use `COPY INTO` to load the data into the table:

```sql
COPY INTO s3_table_csv
  FROM @s3_stage
  FILE_FORMAT = (TYPE = 'CSV');
```

For more details on importing CSV files staged in S3 into Snowflake, check the
[Snowflake documentation](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table#type-csv).

{{< /tab >}}

{{< /tabs >}}


## Step 5. (Optional) Add scheduling

Bulk exports to Amazon S3 using the `COPY TO` command are _one-shot_: every time
you want to export results, you must run the command. To automate running bulk
exports from Materialize to Snowflake on a regular basis, you can set up
scheduling, for example using a simple `cron`-like service or an orchestration
platform like Airflow or Dagster.
