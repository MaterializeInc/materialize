---
title: "COPY TO"
description: "`COPY TO` outputs results from Materialize to standard output or object storage."
menu:
    main:
        parent: "commands"
---

`COPY TO` outputs results from Materialize to standard output or object storage.
This command is useful to output [`SUBSCRIBE`](/sql/subscribe/) results
[to `stdout`](#copy-to-stdout), or perform [bulk exports to Amazon S3](#copy-to-s3).

## Syntax
{{< tabs >}}
{{< tab "Copy to stdout" >}}
### Copy to `stdout`  {#copy-to-stdout}

Copying results to `stdout` is useful to output the stream of updates from a
[`SUBSCRIBE`](/sql/subscribe/) command in interactive SQL clients like `psql`.

{{% include-syntax file="examples/copy_to" example="syntax-stdout" %}}

{{< /tab >}}
{{< tab "Copy to Amazon S3 and S3 compatible services" >}}
### Copy to Amazon S3 and S3 compatible services {#copy-to-s3}

Copying results to Amazon S3 (or S3-compatible services) is useful to perform
tasks like periodic backups for auditing, or downstream processing in
analytical data warehouses like Snowflake, Databricks or BigQuery. For
step-by-step instructions, see the integration guide for [Amazon S3](/serve-results/s3/).

The `COPY TO` command is _one-shot_: every time you want to export results, you
must run the command. To automate exporting results on a regular basis, you can
set up scheduling, for example using a simple `cron`-like service or an
orchestration platform like Airflow or Dagster.

{{% include-syntax file="examples/copy_to" example="syntax-s3" %}}


{{< /tab >}}
{{< /tabs >}}

## Details

### Copy to S3: CSV {#copy-to-s3-csv}

#### Writer settings

{{% include-from-yaml data="examples/copy_to" name="csv-writer-settings" %}}

### Copy to S3: Parquet {#copy-to-s3-parquet}

#### Writer settings

{{% include-from-yaml data="examples/copy_to" name="parquet-writer-settings" %}}

#### Parquet data types

{{% include-from-yaml data="examples/copy_to" name="parquet-data-types" %}}

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/copy-to" %}}

## Examples

### Copy to stdout {#copy-to-stdout-examples}

```mzsql
COPY (SUBSCRIBE some_view) TO STDOUT WITH (FORMAT binary);
```

### Copy to S3 {#copy-to-s3-examples}

#### File format Parquet

```mzsql
COPY some_view TO 's3://mz-to-snow/parquet/'
WITH (
    AWS CONNECTION = aws_role_assumption,
    FORMAT = 'parquet'
  );
```

{{< include-from-yaml data="examples/copy_to" name="parquet-writer-settings" >}}

See also [Copy to S3: Parquet Data Types](#parquet-data-types).

#### File format CSV

```mzsql
COPY some_view TO 's3://mz-to-snow/csv/'
WITH (
    AWS CONNECTION = aws_role_assumption,
    FORMAT = 'csv'
  );
```

{{% include-from-yaml data="examples/copy_to" name="csv-writer-settings" %}}


## Related pages

- [`CREATE CONNECTION`](/sql/create-connection)
- Integration guides:
  - [Amazon S3](/serve-results/s3/)
  - [Snowflake (via S3)](/serve-results/snowflake/)
