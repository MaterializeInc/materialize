---
audience: developer
canonical_url: https://materialize.com/docs/serve-results/sink/
complexity: advanced
description: Sinking results from Materialize to external systems.
doc_type: reference
keywords:
- S3
- strongly
- Sink results
- CREATE A
- CREATE AN
- materialized
- CREATE AND
- AWS Services
product_area: Sinks
status: beta
title: Sink results
---

# Sink results

## Purpose
Sinking results from Materialize to external systems.

If you need to understand the syntax and options for this command, you're in the right place.


Sinking results from Materialize to external systems.


A [sink](/concepts/sinks/) describes the external system you want Materialize to
write data to and details the encoding of that data. You can sink data from a
**materialized** view, a source, or a table.

## Sink methods

To create a sink, you can:

<!-- Dynamic table: sink_external_systems - see original docs -->

### Operational guideline

- Avoid putting sinks on the same cluster that hosts sources to allow for
[blue/green deployment](/manage/dbt/blue-green-deployments).

### Troubleshooting

For help, see [Troubleshooting
sinks](/serve-results/sink/sink-troubleshooting/).


---

## Amazon S3


This guide walks you through the steps required to export results from
Materialize to Amazon S3. Copying results to S3 is
useful to perform tasks like periodic backups for auditing, or downstream
processing in analytical data warehouses like [Snowflake](/serve-results/snowflake/),
Databricks or BigQuery.

## Before you begin

- Ensure you have access to an AWS account, and permissions to create and manage
  IAM policies and roles. If you're not an account administrator, you will need
  support from one!

## Step 1. Set up an Amazon S3 bucket

First, you must set up an S3 bucket and give Materialize enough permissions to
write files to it. We **strongly** recommend using [role assumption-based authentication](/sql/create-connection/#aws-permissions)
to manage access to the target bucket.

### Create a bucket

1. Log in to your AWS account.

1. Navigate to **AWS Services**, then **S3**.

1. Create a new, general purpose S3 bucket with the suggested default
   configurations.

### Create an IAM policy

Once you create an S3 bucket, you must associate it with an [IAM policy](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html)
that specifies what actions can be performed on the bucket by the Materialize
exporter role. For Materialize to be able to write data into the bucket, the
IAM policy must allow the following actions:

Action type  | Action name                                                                            | Action description
-------------|----------------------------------------------------------------------------------------|---------------
Write        | [`s3:PutObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)      | Grants permission to add an object to a bucket.
List         | [`s3:ListBucket`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html) | Grants permission to list some or all of the objects in a bucket.
Write        | [`s3:DeleteObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html)| Grants permission to remove an object from a bucket.

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
                 "s3:DeleteObject"
               ],
               "Resource": "arn:aws:s3:::<bucket>/<prefix>/*"
           },
           {
               "Effect": "Allow",
               "Action": [
                   "s3:ListBucket"
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
   ```text

1. Click **Next**.

1. Enter a name for the policy, and click **Create policy**.

### Create an IAM role

Next, you must attach the policy you just created to a Materialize-specific
[IAM role](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html).

1. Navigate to **AWS Services**, then **AWS IAM**.

1. In the **IAM Dashboard**, click **Roles**, then **Create role**.

1. In **Trusted entity type**, select **Custom trust policy**, and copy and
   paste the policy below.

   ```json
   {
       "Version": "2012-10-17",
       "Statement": [
           {
               "Effect": "Allow",
               "Principal": {
                   "AWS": "arn:aws:iam::664411391173:role/MaterializeConnection"
               },
               "Action": "sts:AssumeRole",
               "Condition": {
                   "StringEquals": {
                       "sts:ExternalId": "PENDING"
                   }
               }
           }
       ]
   }
   ```text

   Materialize **always uses the provided IAM principal** to assume the role, and
   also generates an **external ID** which uniquely identifies your AWS connection
   across all Materialize regions (see [AWS connection permissions](/sql/create-connection/#aws-permissions)).
   For now, you'll set this ID to a dummy value; later, you'll update it with
   the unique identifier for your Materialize region.

1. Click **Next**.

1. In **Add permissions**, select the IAM policy you created in [Create an IAM policy](#create-an-iam-policy),
   and click **Next**.

1. Enter a name for the role, and click **Create role**.

1. Click **View role** to see the role summary page, and note down the
   role **ARN**. You will need it in the next step to create an AWS connection in
   Materialize.

## Step 2. Create a connection

1. In the [SQL Shell](/console/), or your preferred SQL
   client connected to Materialize, create an [AWS connection](/sql/create-connection/#aws),
   replacing `<account-id>` with the 12-digit number that identifies your
   AWS account, and  `<role>` with the name of the role you created in the
   previous step:

   ```mzsql
   CREATE CONNECTION aws_connection
      TO AWS (ASSUME ROLE ARN = 'arn:aws:iam::<account-id>:role/<role>');
   ```text

1. Retrieve the external ID for the connection:

   ```mzsql
   SELECT awsc.id, external_id
    FROM mz_internal.mz_aws_connections awsc
    JOIN mz_connections c ON awsc.id = c.id
    WHERE c.name = 'aws_connection';
   ```text

   The external ID will have the following format:

   ```text
   mz_XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX_uXXX
   ```text

1. In your AWS account, find the IAM role you created in [Create an IAM role](#create-an-iam-role)
   and, under **Trust relationships**, click **Edit trust policy**. Use the
   `external_id` from the previous step to update the trust policy's
   `sts:ExternalId`, then click **Update policy**.

   > **Warning:** 
   Failing to constrain the external ID in your role trust policy
   will allow other Materialize customers to assume your role and use AWS
   privileges you have granted the role!
   

1. Back in Materialize, validate the AWS connection you created using the
   [`VALIDATE CONNECTION`](/sql/validate-connection) command.

   ```mzsql
   VALIDATE CONNECTION aws_connection;
   ```text

   If no validation error is returned, you're ready to use this connection to
   run a bulk export from Materialize to your target S3 bucket! 🔥

## Step 3. Run a bulk export

To export data to your target S3 bucket, use the [`COPY TO`](/sql/copy-to/#copy-to-s3)
command, and the AWS connection you created in the previous step.

#### Parquet

```mzsql
COPY some_object TO 's3://<bucket>/<path>'
WITH (
    AWS CONNECTION = aws_connection,
    FORMAT = 'parquet'
  );
```text

For details on the Parquet writer settings Materialize uses, as well as data
type support and conversion, check the [reference documentation](/sql/copy-to/#copy-to-s3-parquet).

#### CSV

```mzsql
COPY some_object TO 's3://<bucket>/<path>'
WITH (
    AWS CONNECTION = aws_connection,
    FORMAT = 'csv'
  );
```text

You might notice that Materialize first writes a sentinel file to the target S3
bucket. When the copy operation is complete, this file is deleted. This allows
using the [`s3:ObjectRemoved` event](https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-how-to-event-types-and-destinations.html#supported-notification-event-types)
to trigger downstream processing.

## Step 4. (Optional) Add scheduling

Bulk exports to Amazon S3 using the `COPY TO` command are _one-shot_: every time
you want to export results, you must run the command. To automate running bulk
exports on a regular basis, you can set up scheduling, for example using a
simple `cron`-like service or an orchestration platform like Airflow or
Dagster.


---

## Census


This guide walks you through the steps required to create a [Census](https://www.getcensus.com/) sync using Materialize.

## Before you begin

In order to build a sync with Census you will need:

* A table, view, materialized view or source within your Materialize account that you would like to export.
* A [Braze](https://www.braze.com/) account. Census supports a number of possible [destinations](https://www.getcensus.com/integrations), we will use Braze as an example.

## Step 1. Set up a Materialize Source

To begin you will need to add your Materialize database as a source in Census.

1. In Census, navigate to **Sources** and then click **New Source**.

1. From the list of connection types, choose **Materialize**.

1. Set the connection parameters using the credentials provided in the [Materialize console](/console/).
   Then click the **Connect** button.

## Step 2. Set up a Destination

Next you will add a destination where data will be sent.

#### Braze

1. In Census, navigate to **Destinations** and then click **New Destination**.

1. From the list of destinations types, choose **Braze**.

1. You will need to supply your Braze URL (which will most likely be `https://rest.iad-03.braze.com`) and a Braze API key.
   The [Census guide for Braze](https://docs.getcensus.com/destinations/braze) will explain how to create an API key with the
   correct permissions. Then click the **Connect**.

## Step 3. Create a Sync

After successfully adding the Materialize source, you can create a sync to send data from Materialize to your downstream destination.

#### Braze

1. In Census, navigate to **Syncs** and then click **New Sync**.

1. Under **Select a Source** choose **Select a Warehouse Table**. Using the drop-down, choose the Materialize source that was
   configured in step 1 as the **Connection**. Using the **Schema** and **Table** drop-downs you can select the
   Materialize object you would like to export.

1. Under **Select a Destination** choose the Braze destination configured in step 2 and select "User" as the **Object**.

1. Under **Select Sync Behavior** can be set to "Update or Create". This will only add and modify new data in Braze but never delete users.

1. Under **Select a Sync Key** select an id column from the Materialize object.

1. Under **Set Up Braze Field Mappings** set any of the columns in the Materialize object to their corresponding fields in the Braze User entity.

1. Click **Next** to see an overview of your sync and click **Create** to create the sync.

## Step 4. Add a Schedule (Optional)

Your Census sync is created and ready to run. It can be invoked manually but a schedule will ensure all new data
is sent to the destination.

1. In Census navigate to **Syncs** and select the sync that was just created.

1. Within your sync toolbar click **Configuration**. In **Sync Trigger > Schedule** you can select from a number of
   difference schedules. If you are using a source or materialized view as your source object, you can choose "Continuous"
   and Census will retrieve new data as soon as it exists within Materialize.


---

## Kafka and Redpanda


<!-- Ported over content from sink-kafka.md. -->

## Connectors

Materialize bundles a **native connector** that allow writing data to Kafka and
Redpanda. When a user defines a sink to Kafka/Redpanda, Materialize
automatically generates the required schema and writes down the stream of
changes to that view or source. In effect, Materialize sinks act as change data
capture (CDC) producers for the given source or view.

For details on the connector, including syntax, supported formats and examples,
refer to [`CREATE SINK`](/sql/create-sink/kafka).

> **Tip:** 

Redpanda uses the same syntax as Kafka [`CREATE SINK`](/sql/create-sink/kafka).


## Features

This section covers features.

### Memory use during creation

During creation, sinks need to load an entire snapshot of the data in memory.

### Automatic topic creation

If the specified Kafka topic does not exist, Materialize will attempt to create
it using the broker's default number of partitions, default replication factor,
default compaction policy, and default retention policy, unless any specific
overrides are provided as part of the [connection
options](/sql/create-sink/kafka#connection-options).

If the connection's [progress
topic](/sql/create-sink/kafka#exactly-once-processing) does not exist,
Materialize will attempt to create it with a single partition, the broker's
default replication factor, compaction enabled, and both size- and time-based
retention disabled. The replication factor can be overridden using the `PROGRESS
TOPIC REPLICATION FACTOR` option when creating a connection [`CREATE
CONNECTION`](/sql/create-connection).

To customize topic-level configuration, including compaction settings and other
values, use the `TOPIC CONFIG` option in the [connection
options](/sql/create-sink/kafka#connection-options) to set any relevant kafka
[topic configs](https://kafka.apache.org/documentation/#topicconfigs).

If you manually create the topic or progress topic in Kafka before
running `CREATE SINK`, observe the following guidance:

| Topic          | Configuration       | Guidance
|----------------|---------------------|---------
| Data topic     | Partition count     | Your choice, based on your performance and ordering requirements.
| Data topic     | Replication factor  | Your choice, based on your durability requirements.
| Data topic     | Compaction          | Your choice, based on your downstream applications' requirements. If using the [Upsert envelope](/sql/create-sink/kafka#upsert), enabling compaction is typically the right choice.
| Data topic     | Retention           | Your choice, based on your downstream applications' requirements.
| Progress topic | Partition count     | **Must be set to 1.** Using multiple partitions can cause Materialize to violate its [exactly-once guarantees](/sql/create-sink/kafka#exactly-once-processing).
| Progress topic | Replication factor  | Your choice, based on your durability requirements.
| Progress topic | Compaction          | We recommend enabling compaction to avoid accumulating unbounded state. Disabling compaction may cause performance issues, but will not cause correctness issues.
| Progress topic | Retention           | **Must be disabled.** Enabling retention can cause Materialize to violate its [exactly-once guarantees](/sql/create-sink/kafka#exactly-once-processing).
| Progress topic | Tiered storage      | We recommend disabling tiered storage to allow for more aggressive data compaction. Fully compacted data requires minimal storage, typically only tens of bytes per sink, making it cost-effective to maintain directly on local disk.
| Progress topic | Segment bytes.      | Defaults to 128 MiB. We recommend going no higher than 256 MiB to avoid
slow startups when creating new sinks, as they must process the entire progress topic on startup.
> **Warning:** 
<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: kafka-sink-drop --> --> -->


### Exactly-once processing

By default, Kafka sinks provide [exactly-once processing guarantees](https://kafka.apache.org/documentation/#semantics), which ensures that messages are not duplicated or dropped in failure scenarios.

To achieve this, Materialize stores some internal metadata in an additional
*progress topic*. This topic is shared among all sinks that use a particular
[Kafka connection](/sql/create-connection/#kafka). The name of the progress
topic can be specified when [creating a
connection](/sql/create-connection/#kafka-options); otherwise, a default name of
`_materialize-progress-{REGION ID}-{CONNECTION ID}` is used. In either case,
Materialize will attempt to create the topic if it does not exist. The contents
of this topic are not user-specified.

#### End-to-end exactly-once processing

Exactly-once semantics are an end-to-end property of a system, but Materialize
only controls the initial produce step. To ensure _end-to-end_ exactly-once
message delivery, you should ensure that:

- The broker is configured with replication factor greater than 3, with unclean
  leader election disabled (`unclean.leader.election.enable=false`).
- All downstream consumers are configured to only read committed data
  (`isolation.level=read_committed`).
- The consumers' processing is idempotent, and offsets are only committed when
  processing is complete.

For more details, see [the Kafka documentation](https://kafka.apache.org/documentation/).

### Partitioning

By default, Materialize assigns a partition to each message using the following
strategy:

  1. Encode the message's key in the specified format.
  2. If the format uses a Confluent Schema Registry, strip out the
     schema ID from the encoded bytes.
  3. Hash the remaining encoded bytes using [SeaHash].
  4. Divide the hash value by the topic's partition count and assign the
     remainder as the message's partition.

If a message has no key, all messages are sent to partition 0.

To configure a custom partitioning strategy, you can use the `PARTITION BY`
option. This option allows you to specify a SQL expression that computes a hash
for each message, which determines what partition to assign to the message:

```sql
-- General syntax.
CREATE SINK ... INTO KAFKA CONNECTION <name> (PARTITION BY = <expression>) ...;

-- Example.
CREATE SINK ... INTO KAFKA CONNECTION <name> (
    PARTITION BY = kafka_murmur2(name || address)
) ...;
```text

The expression:
  * Must have a type that can be assignment cast to [`uint8`].
  * Can refer to any column in the sink's underlying relation when using the
    [upsert envelope](/sql/create-sink/kafka#upsert-envelope).
  * Can refer to any column in the sink's key when using the
    [Debezium envelope](/sql/create-sink/kafka#debezium-envelope).

Materialize uses the computed hash value to assign a partition to each message
as follows:

  1. If the hash is `NULL` or computing the hash produces an error, assign
     partition 0.
  2. Otherwise, divide the hash value by the topic's partition count and assign
     the remainder as the message's partition (i.e., `partition_id = hash %
     partition_count`).

Materialize provides several [hash functions](/sql/functions/#hash-functions)
which are commonly used in Kafka partition assignment:

  * `crc32`
  * `kafka_murmur2`
  * `seahash`

For a full example of using the `PARTITION BY` option, see [Custom
partioning](/sql/create-sink/kafka#custom-partitioning).

### Kafka transaction markers

Materialize uses [Kafka
transactions](https://www.confluent.io/blog/transactions-apache-kafka/). When
Kafka transactions are used, special control messages known as **transaction
markers** are published to the topic. Transaction markers inform both the broker
and clients about the status of a transaction. When a topic is read using a
standard Kafka consumer, these markers are not exposed to the application, which
can give the impression that some offsets are being skipped.


---

## S3 Compatible Object Storage


This guide walks you through the steps required to export results from
Materialize to an S3 compatible object storage service, such as Google
Cloud Storage, or Cloudflare R2.

## Before you begin:
- Make sure that you have setup your bucket.
- Obtain the following for your bucket. Instructions to obtain these vary by provider.
  - The S3 compatible URI (`S3_BUCKET_URI`)
  - The S3 compatible access tokens (`ACCESS_KEY_ID` and `SECRET_ACCESS_KEY`)

## Step 1. Create a connection

1. In the [SQL Shell](/console/), or your preferred SQL
   client connected to Materialize, create an [AWS connection](/sql/create-connection/#aws),
   replacing `<ACCESS_KEY_ID>` and  `<SECRET_ACCESS_KEY>` with the credentials for your bucket. The AWS
   connection can be used to connect to any S3 compatible object storage service, by specifying the endpoint and the region.

   For example, to connect to Google Cloud Storage, you can run the following:

    ```mzsql
    CREATE SECRET secret_access_key AS '<SECRET_ACCESS_KEY>';
    CREATE CONNECTION bucket_connection TO AWS (
        ACCESS KEY ID = '<ACCESS_KEY_ID>',
        SECRET ACCESS KEY = SECRET secret_access_key,
        ENDPOINT = 'https://storage.googleapis.com',
        REGION = 'us'
    );
    ```text

> **Warning:** 
  `VALIDATE CONNECTION` only works for AWS S3 connections. Using `VALIDATE CONNECTION` to test a connection to S3 compatible object storage service will result in an error. However, you can still use the connection to copy data.


## Step 2. Run a bulk export

To export data to your target bucket, use the [`COPY TO`](/sql/copy-to/#copy-to-s3)
command and the AWS connection you created in the previous step. Replace the `<S3_BUCKET_URI>`
with the S3 compatible URI for your target bucket.

#### Parquet

```mzsql
COPY some_object TO '<S3_BUCKET_URI>'
WITH (
    AWS CONNECTION = bucket_connection,
    FORMAT = 'parquet'
  );
```text

For details on the Parquet writer settings Materialize uses, as well as data
type support and conversion, check the [reference documentation](/sql/copy-to/#copy-to-s3-parquet).

#### CSV

```mzsql
COPY some_object TO '<S3_BUCKET_URI>'
WITH (
    AWS CONNECTION = bucket_connection,
    FORMAT = 'csv'
  );
```bash

## Step 3. (Optional) Add scheduling

Bulk exports to object storage using the `COPY TO` command are _one-shot_: every time
you want to export results, you must run the command. To automate running bulk
exports on a regular basis, you can set up scheduling, for example using a
simple `cron`-like service or an orchestration platform like Airflow or
Dagster.


---

## Snowflake


[//]: # "TODO(morsapaes) For Kafka users, it's possible to sink data to
Snowflake continuously using the Snowflake connector for Kafka. We should also
document that approach."

> **Public Preview:** This feature is in public preview.

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

This section covers step 2. configure a snowflake storage integration.

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
   ```text

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

> **Note:** 
Only users with either the [`ACCOUNTADMIN` role](https://docs.snowflake.com/en/user-guide/security-access-control-considerations#using-the-accountadmin-role),
or a role with the [global `CREATE INTEGRATION` privilege](https://docs.snowflake.com/en/user-guide/security-access-control-privileges#global-privileges-account-level-privileges)
can execute this step.


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
   ```text

1. Retrieve the IAM principal for your Snowflake account using the
   [`DESC INTEGRATION`](https://docs.snowflake.com/en/sql-reference/sql/desc-integration)
   command:

   ```sql
   DESC INTEGRATION s3_int;
   ```text

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
```text

> **Note:** 
To create a stage that uses a storage integration, the active user must have a
role with the [`CREATE STAGE` privilege](https://docs.snowflake.com/en/sql-reference/sql/create-stage)
for the active schema, as well as the [`USAGE` privilege](https://docs.snowflake.com/en/sql-reference/sql/grant-privilege#syntax)
on the relevant storage integration.


## Step 4. Import data into Snowflake

To import the data stored in S3 into Snowflake, you can then create a table and
use the [`COPY INTO`](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table)
command to load it from the external stage.

#### Parquet

Create a table with a single column of type [`VARIANT`](https://docs.snowflake.com/en/sql-reference/data-types-semistructured#variant):

```sql
CREATE TABLE s3_table_parquet (
    col VARIANT
);
```text

Use `COPY INTO` to load the data into the table:

```sql
COPY INTO s3_table_parquet
  FROM @S3_stage
  FILE_FORMAT = (TYPE = 'PARQUET');
```text

For more details on importing Parquet files staged in S3 into Snowflake, check the
[Snowflake documentation](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table#type-parquet).

#### CSV

Create a table with the same number of columns as the number of delimited
columns in the input data file:

```sql
CREATE TABLE s3_table_csv (
    col_1 INT,
    col_2 TEXT,
    col_3 TIMESTAMP
);
```text

Use `COPY INTO` to load the data into the table:

```sql
COPY INTO s3_table_csv
  FROM @s3_stage
  FILE_FORMAT = (TYPE = 'CSV');
```text

For more details on importing CSV files staged in S3 into Snowflake, check the
[Snowflake documentation](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table#type-csv).


## Step 5. (Optional) Add scheduling

Bulk exports to Amazon S3 using the `COPY TO` command are _one-shot_: every time
you want to export results, you must run the command. To automate running bulk
exports from Materialize to Snowflake on a regular basis, you can set up
scheduling, for example using a simple `cron`-like service or an orchestration
platform like Airflow or Dagster.


---

## Troubleshooting sinks


<!-- Copied over from the old manage/troubleshooting guide -->
## Why isn't my sink exporting data?
First, look for errors in [`mz_sink_statuses`](/sql/system-catalog/mz_internal/#mz_sink_statuses):

```mzsql
SELECT * FROM mz_internal.mz_sink_statuses
WHERE name = <SINK_NAME>;
```text

If your sink reports a status of `stalled` or `failed`, you likely have a
configuration issue. The returned `error` field will provide details.

If your sink reports a status of `starting` for more than a few minutes,
[contact support](/support).

## How do I monitor sink ingestion progress?

Repeatedly query the
[`mz_sink_statistics`](/sql/system-catalog/mz_internal/#mz_sink_statistics)
table and look for ingestion statistics that advance over time:

```mzsql
SELECT
    messages_staged,
    messages_committed,
    bytes_staged,
    bytes_committed
FROM mz_internal.mz_sink_statistics
WHERE id = <SINK ID>;
```

(You can also look at statistics for individual worker threads to evaluate
whether ingestion progress is skewed, but it's generally simplest to start
by looking at the aggregate statistics for the whole source.)

The `messages_staged` and `bytes_staged` statistics should roughly correspond
with what materialize has written (but not necessarily committed) to the
external service. For example, the `bytes_staged` and `messages_staged` fields
for a Kafka sink should roughly correspond with how many messages materialize
has written to the Kafka topic, and how big they are (including the key), but
the Kafka transaction for those messages might not have been committed yet.

`messages_committed` and `bytes_committed` correspond to the number of messages
committed to the external service. These numbers can be _smaller_ than the
`*_staged` statistics, because Materialize might fail to write transactions and
retry them.

If any of these statistics are not making progress, your sink might be stalled
or need to be scaled up.

If the `*_staged` statistics are making progress, but the `*_committed` ones
are not, there may be a configuration issues with the external service that is
preventing Materialize from committing transactions. Check the `reason`
column in `mz_sink_statuses`, which can provide more information.