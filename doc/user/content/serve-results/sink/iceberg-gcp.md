---
title: "GCP BigLake"
description: "How to export results from Materialize to Apache Iceberg tables on Google Cloud BigLake."
menu:
  main:
    parent: sink-iceberg
    name: "GCP BigLake"
    weight: 20
---

{{< public-preview />}}

This guide walks you through the steps required to set up Iceberg sinks in
Materialize Cloud.

## Prerequisites

Google Cloud [documents the Lakehouse/BigLake setup process here](https://docs.cloud.google.com/lakehouse/docs/lakehouse-iceberg-rest-catalog). The parts you'll need:
- A Google Cloud project with the BigLake API enabled.
- A Google Cloud Storage bucket to serve as the Iceberg warehouse.
- A Lakehouse runtime catalog backed by your warehouse bucket.
  - _NOTE: Materialize uses a service account key, not catalog-vended credentials, to write Iceberg data files._
  So you may configure your catalog with either "End-user credentials" or "Credential vending mode".
- A namespace in the BigLake catalog.

## Create the Iceberg catalog connection in Materialize

### Step 1. Set up permissions in GCP

Materialize authenticates to BigLake as a Google Cloud [service
account](https://docs.cloud.google.com/iam/docs/service-account-overview) you own.

1. Create the service account.
2. Grant the service account these roles on your **project**:
    - `biglake.editor` (BigLake Editor)
    - `serviceusage.serviceUsageConsumer` (Service Usage Consumer)
3. Grant the service account this role on your **Iceberg warehouse bucket**:
    - `storage.objectUser` (Storage Object User)
4. [Create a service account key.](https://docs.cloud.google.com/iam/docs/keys-create-delete#iam-service-account-keys-create-gcloud)

### Step 2. Create a GCP connection and Iceberg catalog connection in Materialize

{{% include-example file="examples/create_connection" example="example-iceberg-catalog-gcp-connection" %}}

## Create the Iceberg sink in Materialize

{{% include-example file="examples/create_sink_iceberg" example="tutorial-create-sink-intro" %}}

### Upsert mode

{{% include-example file="examples/create_sink_iceberg" example="tutorial-create-sink-upsert-mode" %}}

### Append mode

{{% include-example file="examples/create_sink_iceberg" example="tutorial-create-sink-append-mode" %}}

## Considerations

### Commit interval tradeoffs {#commit-interval-tradeoffs}

{{% include-headless "/headless/iceberg-sinks/commit-interval-tradeoffs" %}}

### Exactly-once delivery

{{< include-from-yaml data="examples/create_sink_iceberg"
name="exactly-once-delivery" >}}

### Type mapping

{{% include-headless
  "/headless/iceberg-sinks/type-mapping" %}}

### Limitations

{{% include-headless "/headless/iceberg-sinks/limitations-list" %}}

## Troubleshooting

{{% include-headless "/headless/iceberg-sinks/troubleshooting" %}}

## Related pages

- [`CREATE SINK`](/sql/create-sink/iceberg)
- [`CREATE CONNECTION`](/sql/create-connection)
- [Apache Iceberg documentation](https://iceberg.apache.org/docs/latest/)
