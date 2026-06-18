---
title: "GCP BigLake"
description: "How to export results from Materialize to Apache Iceberg tables on Google Cloud BigLake."
menu:
  main:
    parent: sink-iceberg
    name: "GCP BigLake"
    weight: 20
---

{{< private-preview />}}

{{< warning >}}
{{< include-from-yaml data="examples/create_sink_iceberg" name="restrictions-limitations-gcp-maintenance" >}}
{{< /warning >}}

This guide walks you through the steps required to set up Iceberg sinks in
Materialize Cloud.

## Prerequisites

- Google Cloud project with the [BigLake API enabled](https://docs.cloud.google.com/lakehouse/docs/lakehouse-iceberg-rest-catalog#before_you_begin).
- Google Cloud [Storage bucket](https://console.cloud.google.com/storage/browser) to serve as the Iceberg warehouse.
- [Lakehouse runtime catalog](https://docs.cloud.google.com/lakehouse/docs/lakehouse-iceberg-rest-catalog#create_a_catalog) backed by your warehouse bucket.
  For **Authentication method**, you can select either _End-user credentials_ or _Credential vending mode_.
  Materialize authenticates separately with a [GCP service account key](https://docs.cloud.google.com/iam/docs/keys-create-delete#iam-service-account-keys-create-gcloud)
  (provided in the next step), so both modes work.
- [Namespace in the catalog](https://docs.cloud.google.com/lakehouse/docs/lakehouse-iceberg-rest-catalog#create_a_namespace_or_schema).

## Create the Iceberg catalog connection in Materialize

### Step 1. Set up permissions in GCP

Materialize uses a Google Cloud [service account](https://docs.cloud.google.com/iam/docs/service-account-overview) to
authenticate to BigLake.

1. Create the [service account](https://console.cloud.google.com/iam-admin/serviceaccounts).
2. Grant the service account these roles on your **project**:
    - `biglake.editor` (BigLake Editor)
    - `serviceusage.serviceUsageConsumer` (Service Usage Consumer)
3. Grant the service account this role on your **Iceberg warehouse bucket**:
    - `storage.objectUser` (Storage Object User)
4. [Create a service account key in JSON format.](https://docs.cloud.google.com/iam/docs/keys-create-delete#iam-service-account-keys-create-gcloud)
5. Base64-encode the entire JSON key (e.g. `base64 < sa_key.json`). You will paste the
   resulting string into the `CREATE SECRET` statement in the next step.

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

- {{< include-from-yaml data="examples/create_sink_iceberg" name="restrictions-limitations-gcp-maintenance" >}}

{{% include-headless "/headless/iceberg-sinks/limitations-list" %}}

## Troubleshooting

{{% include-headless "/headless/iceberg-sinks/troubleshooting" %}}

## Related pages

- [`CREATE SINK`](/sql/create-sink/iceberg)
- [`CREATE CONNECTION`](/sql/create-connection)
- [Apache Iceberg documentation](https://iceberg.apache.org/docs/latest/)
