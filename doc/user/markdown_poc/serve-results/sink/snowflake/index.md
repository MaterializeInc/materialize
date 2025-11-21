<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [Serve results](/docs/serve-results/)  /  [Sink
results](/docs/serve-results/sink/)

</div>

# Snowflake

<div class="public-preview">

**PREVIEW** This feature is in **[public
preview](https://materialize.com/preview-terms/)**. It is under active
development and may have stability or performance issues. It isn't
subject to our backwards compatibility guarantees.

</div>

This guide walks you through the steps required to bulk-export results
from Materialize to Snowflake using Amazon S3 as the intermediate object
store.

## Before you begin

- Ensure you have access to an AWS account, and permissions to create
  and manage IAM policies and roles. If you’re not an account
  administrator, you will need support from one!

- Ensure you have access to a Snowflake account, and are able to connect
  as a user with either the [`ACCOUNTADMIN`
  role](https://docs.snowflake.com/en/user-guide/security-access-control-considerations#using-the-accountadmin-role),
  or a role with the [global `CREATE INTEGRATION`
  privilege](https://docs.snowflake.com/en/user-guide/security-access-control-privileges#global-privileges-account-level-privileges).

## Step 1. Set up bulk exports to Amazon S3

Follow the [Amazon S3 integration guide](/docs/serve-results/s3/) to set
up an Amazon S3 bucket that Materialize securely writes data into. This
will be your starting point for bulk-loading Materialize data into
Snowflake.

## Step 2. Configure a Snowflake storage integration

### Create an IAM policy

To bulk-load data from an S3 bucket into Snowflake, you must create a
new [IAM
policy](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html)
that specifies what actions can be performed on the bucket by the
Snowflake importer role. For Snowflake to be able to read data from the
bucket, the IAM policy must allow the following actions:

| Action type | Action name | Action description |
|----|----|----|
| Write | [`s3:GetBucketLocation`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html) | Grants permission to return the region the bucket is hosted in. |
| Read | [`s3:GetObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html) | Grants permission to retrieve objects from a bucket. |
| Read | [`s3:GetObjectVersion`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html) | Grants permission to retrieve a specific version of an object from a bucket. |
| List | [`s3:ListBucket`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html) | Grants permission to list some or all of the objects in a bucket. |
| Write | [`s3:DeleteObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html) | (Optional) Grants permission to remove an object from a bucket. |
| Write | [`s3:DeleteObjectVersion`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html) | (Optional) Grants permission to remove a specific version of an object from a bucket. |
| Write | [`s3:PutObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html) | (Optional) Grants permission to add an object to a bucket. |

To create a new IAM policy:

1.  Navigate to **AWS Services**, then **AWS IAM**.

2.  In the **IAM Dashboard**, click **Policies**, then **Create
    policy**.

3.  For **Policy editor**, choose **JSON**.

4.  Copy and paste the policy below into the editor, replacing
    `<bucket>` with the bucket name and `<prefix>` with the folder path
    prefix.

    <div class="highlight">

    ``` chroma
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

    </div>

5.  Click **Next**.

6.  Enter a name for the policy, and click **Create policy**.

### Create an IAM role

Next, you must attach the policy you just created to a
Snowflake-specific [IAM
role](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html).

1.  Navigate to **AWS Services**, then **AWS IAM**.

2.  In the **IAM Dashboard**, click **Roles**, then **Create role**.

3.  In **Trusted entity type**, select **Account ID**, then **This
    account**. Later, you’ll update it with the unique identifier for
    your Snowflake account.

4.  Check the **Require external ID** box. Enter a placeholder
    **External ID** (e.g. 0000). Later, you’ll update it with the unique
    external ID for your Snowflake storage integration.

5.  Click **Next**.

6.  In **Add permissions**, select the IAM policy you created in [Create
    an IAM policy](#create-an-iam-policy), and click **Next**.

7.  Enter a name for the role, and click **Create role**.

8.  Click **View role** to see the role summary page, and note down the
    role **ARN**. You will need it in the next step to create a
    Snowflake storage integration.

### Create a Snowflake storage integration

<div class="note">

**NOTE:** Only users with either the [`ACCOUNTADMIN`
role](https://docs.snowflake.com/en/user-guide/security-access-control-considerations#using-the-accountadmin-role),
or a role with the [global `CREATE INTEGRATION`
privilege](https://docs.snowflake.com/en/user-guide/security-access-control-privileges#global-privileges-account-level-privileges)
can execute this step.

</div>

1.  In [Snowsight](https://app.snowflake.com/), or your preferred SQL
    client connected to Snowflake, create a [storage
    integration](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration),
    replacing `<role>` with the name of the role you created in the
    previous step:

    <div class="highlight">

    ``` chroma
    CREATE STORAGE INTEGRATION S3_int
      TYPE = EXTERNAL_STAGE
      STORAGE_PROVIDER = 'S3'
      ENABLED = TRUE
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::001234567890:role/<role>'
      STORAGE_ALLOWED_LOCATIONS = ('*');
    ```

    </div>

2.  Retrieve the IAM principal for your Snowflake account using the
    [`DESC INTEGRATION`](https://docs.snowflake.com/en/sql-reference/sql/desc-integration)
    command:

    <div class="highlight">

    ``` chroma
    DESC INTEGRATION s3_int;
    ```

    </div>

    Note down the values for the `STORAGE_AWS_IAM_USER_ARN` and
    `STORAGE_AWS_EXTERNAL_ID` properties. You will need them in the next
    step to update the Snowflake trust policy attached to your S3
    bucket.

### Update the IAM policy

1.  In your AWS account, find the IAM role you created in [Create an IAM
    role](#create-an-iam-role) and, under **Trust relationships**, click
    **Edit trust policy**. Use the values for the
    `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` properties
    from the previous step to update the trust policy’s `Principal` and
    `ExternalId`, then click **Update policy**.

## Step 3. Create a Snowflake external stage

Back in Snowflake, create an [external
stage](https://docs.snowflake.com/en/user-guide/data-load-S3-create-stage#external-stages)
that uses the storage integration above and references your S3 bucket.

<div class="highlight">

``` chroma
CREATE STAGE s3_stage
  STORAGE_INTEGRATION = s3_int
  URL = 's3://<bucket>/<prefix>/';
```

</div>

<div class="note">

**NOTE:** To create a stage that uses a storage integration, the active
user must have a role with the [`CREATE STAGE`
privilege](https://docs.snowflake.com/en/sql-reference/sql/create-stage)
for the active schema, as well as the [`USAGE`
privilege](https://docs.snowflake.com/en/sql-reference/sql/grant-privilege#syntax)
on the relevant storage integration.

</div>

## Step 4. Import data into Snowflake

To import the data stored in S3 into Snowflake, you can then create a
table and use the
[`COPY INTO`](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table)
command to load it from the external stage.

<div class="code-tabs">

<div class="tab-content">

<div id="tab-parquet" class="tab-pane" title="Parquet">

Create a table with a single column of type
[`VARIANT`](https://docs.snowflake.com/en/sql-reference/data-types-semistructured#variant):

<div class="highlight">

``` chroma
CREATE TABLE s3_table_parquet (
    col VARIANT
);
```

</div>

Use `COPY INTO` to load the data into the table:

<div class="highlight">

``` chroma
COPY INTO s3_table_parquet
  FROM @S3_stage
  FILE_FORMAT = (TYPE = 'PARQUET');
```

</div>

For more details on importing Parquet files staged in S3 into Snowflake,
check the [Snowflake
documentation](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table#type-parquet).

</div>

<div id="tab-csv" class="tab-pane" title="CSV">

Create a table with the same number of columns as the number of
delimited columns in the input data file:

<div class="highlight">

``` chroma
CREATE TABLE s3_table_csv (
    col_1 INT,
    col_2 TEXT,
    col_3 TIMESTAMP
);
```

</div>

Use `COPY INTO` to load the data into the table:

<div class="highlight">

``` chroma
COPY INTO s3_table_csv
  FROM @s3_stage
  FILE_FORMAT = (TYPE = 'CSV');
```

</div>

For more details on importing CSV files staged in S3 into Snowflake,
check the [Snowflake
documentation](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table#type-csv).

</div>

</div>

</div>

## Step 5. (Optional) Add scheduling

Bulk exports to Amazon S3 using the `COPY TO` command are *one-shot*:
every time you want to export results, you must run the command. To
automate running bulk exports from Materialize to Snowflake on a regular
basis, you can set up scheduling, for example using a simple `cron`-like
service or an orchestration platform like Airflow or Dagster.

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/serve-results/sink/snowflake.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
