---
title: "Google Cloud Storage (GCS)"
description: "How to export results from Materialize to Google Cloud Storage (GCS)."
menu:
  main:
    parent: sink
    name: "Google Cloud Storage (GCS)"
    weight: 12
---

{{< public-preview />}}

This guide walks you through the steps required to export results from
Materialize to Google Cloud Storage (GCS). Copying results to GCS is
useful to perform tasks like periodic backups for auditing, or downstream
processing in analytical data warehouses like [Snowflake](/serve-results/snowflake/),
Databricks or [BigQuery](/serve-results/bigquery/).

{{< note >}}
Although Materialize does not integrate natively with GCS, GCS is interoperable
with Amazon S3 via the [Cloud Storage XML API](https://cloud.google.com/storage/docs/interoperability#xml_api).
This allows you to export data to GCS using an [AWS connection](/sql/create-connection/#aws)
with an HMAC key and the [`COPY TO`](/sql/copy-to/#copy-to-s3)
command.
{{< /note >}}

## Before you begin

- Ensure you have access to a Google Cloud Platform (GCP) account, and
  permissions to manage [hash-based message authentication code
  (HMAC) keys](https://cloud.google.com/storage/docs/authentication/hmackeys) and [service accounts](https://cloud.google.com/iam/docs/understanding-roles#iam.serviceAccountCreator).
  If you don't have these permissions for the selected project, you
  will need support from a user that does!

## Step 1. Set up a GCS bucket

First, you must set up a GCS bucket and give Materialize enough permissions to
read, write, and delete files in it. Because the integration uses the Cloud
Storage XML API and credentials-based authentication (rather than role
assumption-based authentication), we **strongly** recommend using a
[service account](https://cloud.google.com/iam/docs/service-accounts-create#creating_a_service_account)
to access the target bucket.

### Create a bucket

1. Log in to your GCP account.

1. Navigate to **All products**, then **Cloud Storage**.

1. Create a new, general purpose bucket with the suggested default
   configurations.

### Create a service account

1. Navigate to **All products**, then **IAM & Admin** > **Service Accounts**.

1. Click **Create service account**. Enter a name for the service account and
click **Create and continue**.

1. In **Select a role**, select **Cloud Storage** and add the **Storage Object
User** role.

1. Click **Continue**.

1. Optionally, grant access to other users or groups that need to perform
actions as this service account.

### Create an HMAC key

1. In **Organization Policies**, ensure that the
[`constraints/storage.restrictAuthTypes` constraint](https://cloud.google.com/storage/docs/org-policy-constraints#restrict-auth-types)
**does not** restrict requests signed by service account HMAC keys (`SERVICE_ACCOUNT_HMAC_SIGNED_REQUESTS`).

1. Back in **Cloud Storage**, click **Settings** > **Interoperability**
   and **Create a key for a service account**.

1. Select the service account you created previously and click **Create key** to
   associate a new HMAC key with the account. Note down the **Access key**
   and **Secret**. You will need these credentials in the next step to create an
   interoperable AWS connection in Materialize.

## Step 2. Create a connection

Although Materialize does not integrate natively with GCS, GCS is interoperable
with Amazon S3 via the [Cloud Storage XML API](https://cloud.google.com/storage/docs/interoperability#xml_api).
This allows you to export data to GCS using the natively supported
[AWS connection](/sql/create-connection/#aws) type with the credentials you
created in the previous step.

1. In the [SQL Shell](https://console.materialize.com/), or your preferred SQL
   client connected to Materialize, create an [AWS connection](/sql/create-connection/#aws),
   replacing `<access-key>` with the access key you created in the previous
   step, `<secret>` with the associated secret value, and `<region>` with the
   region the GCS bucket is hosted in:

   ```mzsql
   CREATE SECRET hmac_secret AS '<secret>';

   CREATE CONNECTION gcp_interop_connection
   TO AWS (
     ACCESS KEY ID = '<access-key>',
     SECRET ACCESS KEY = SECRET hmac_secret,
     ENDPOINT = 'https://storage.googleapis.com',
     REGION = '<region>'
   );
   ```

1. Back in Materialize, validate the connection you created using the
   [`VALIDATE CONNECTION`](/sql/validate-connection) command.

   ```mzsql
   VALIDATE CONNECTION gcp_interop_connection;
   ```

   If no validation error is returned, you're ready to use this connection to
   run a bulk export from Materialize to your target GCS bucket! 🔥

## Step 3. Run a bulk export

To export data to your target GCS bucket, use the [`COPY TO`](/sql/copy-to/#copy-to-s3)
command, and the connection you created in the previous step.

{{< tabs >}}
{{< tab "Parquet">}}

```mzsql
COPY some_object TO 's3://<bucket>/<path>'
WITH (
    AWS CONNECTION = gcp_interop_connection,
    FORMAT = 'parquet'
  );
```

For details on the Parquet writer settings Materialize uses, as well as data
type support and conversion, check the [reference documentation](/sql/copy-to/#copy-to-s3-parquet).

{{< /tab >}}

{{< tab "CSV">}}

```mzsql
COPY some_object TO 's3://<bucket>/<path>'
WITH (
    AWS CONNECTION = gcp_interop_connection,
    FORMAT = 'csv'
  );
```

{{< /tab >}}

{{< /tabs >}}

You might notice that Materialize first writes a sentinel file to the target GCS
bucket. When the copy operation is complete, this file is deleted. This allows
using the [`google.cloud.storage.object.v1.deleted` event](https://cloud.google.com/functions/docs/calling/storage)
to trigger downstream processing.

## Step 4. (Optional) Add scheduling

Bulk exports to GCS using the `COPY TO` command are _one-shot_: every time
you want to export results, you must run the command. To automate running bulk
exports on a regular basis, you can set up scheduling, for example using a
simple `cron`-like service or an orchestration platform like Airflow or
Dagster.
