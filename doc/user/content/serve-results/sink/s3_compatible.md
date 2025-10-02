---
title: "S3 Compatible Object Storage"
description: "How to export results from Materialize to S3 compatible object storage"
aliases:
  - /serve-results/s3-compatible/
menu:
  main:
    parent: sink
    name: "S3 compatible storage"
    weight: 10
---

{{< public-preview />}}

This guide walks you through the steps required to export results from
Materialize to an S3 compatible object storage service, such as Google
Cloud Storage, or Cloudflare R2.

## Before you begin:
- Make sure that you have setup your bucket
- Obtain the S3 compatible URI for your bucket, as well as S3 access tokens (`ACCESS_KEY_ID` and `SECRET_ACCESS_KEY`). Instructions to obtain these vary by provider.

## Step 1. Create a connection

1. In the [SQL Shell](https://console.materialize.com/), or your preferred SQL
   client connected to Materialize, create an [AWS connection](/sql/create-connection/#aws),
   replacing `<ACCESS_KEY_ID>` and  `<SECRET_ACCESS_KEY>` with the credentials for your bucket. The AWS
   connection can be used to connect to any S3 compatible object storage service.

    ```mzsql
    CREATE SECRET secret_access_key AS '<SECRET_ACCESS_KEY>';
    CREATE CONNECTION bucket_connection TO AWS (
        ACCESS KEY ID = '<ACCESS_KEY_ID>',
        SECRET ACCESS KEY = SECRET secret_access_key
    );
    ```

1. Validate the connection you created using the
   [`VALIDATE CONNECTION`](/sql/validate-connection) command.

   ```mzsql
   VALIDATE CONNECTION bucket_connection;
   ```

   If no validation error is returned, you're ready to use this connection to
   run a bulk export from Materialize to your target bucket.

## Step 2. Run a bulk export

To export data to your target bucket, use the [`COPY TO`](/sql/copy-to/#copy-to-s3)
command, and the AWS connection you created in the previous step. Replace the '<S3_BUCKET_URI>'
with the S3 compatible URI for your target bucket.

{{< tabs >}}
{{< tab "Parquet">}}

```mzsql
COPY some_object TO '<S3_BUCKET_URI>'
WITH (
    AWS CONNECTION = bucket_connection,
    FORMAT = 'parquet'
  );
```

For details on the Parquet writer settings Materialize uses, as well as data
type support and conversion, check the [reference documentation](/sql/copy-to/#copy-to-s3-parquet).

{{< /tab >}}

{{< tab "CSV">}}

```mzsql
COPY some_object TO '<S3_BUCKET_URI>'
WITH (
    AWS CONNECTION = bucket_connection,
    FORMAT = 'csv'
  );
```

{{< /tab >}}

{{< /tabs >}}

## Step 4. (Optional) Add scheduling

Bulk exports to Amazon object storage the `COPY TO` command are _one-shot_: every time
you want to export results, you must run the command. To automate running bulk
exports on a regular basis, you can set up scheduling, for example using a
simple `cron`-like service or an orchestration platform like Airflow or
Dagster.
