---
title: "Fine tune for sub-second freshness"
description: "How to lower the tick rate to get sub-second freshness, and how to verify it by measuring end-to-end latency."
menu:
  main:
    name: "Fine tune for sub-second freshness"
    identifier: subsecond-freshness
    parent: freshness
    weight: 10
---

To get sub-second [freshness](/fundamentals/concepts/reaction-time/#freshness),
lower the tick rate of your sources and tables, then verify the result by
measuring end-to-end latency.

## How it works

Materialize assigns timestamps to data from sources and tables at a fixed
interval, the **tick rate** (or timestamp interval). The default is `1s`. A
change becomes visible to queries only after the next tick, so the tick rate
sets a floor on freshness. Views, indexes, and materialized views advance at the
tick rate of the sources and tables they depend on.

Lowering the tick rate makes changes visible sooner, but puts more strain on
the system. Each tick advances the frontier of every source and table, which
writes to Materialize's storage layer and its metadata store. A shorter interval
increases that write load, especially in environments with many sources and
tables. Test the tick rate you choose with your workload before you roll it out.

Tables use the environment-level default tick rate. Each source takes the
default when it is created, and can override it with the `TIMESTAMP INTERVAL`
option of [`CREATE SOURCE`](/sql/create-source/) or
[`ALTER SOURCE`](/sql/alter-source/). The following environment-level
parameters control the tick rate:

| Parameter | Default | Description |
| --------- | ------- | ----------- |
| `default_timestamp_interval` | `1s` | The tick rate for tables, and for sources created without a `TIMESTAMP INTERVAL` option. |
| `min_timestamp_interval` | `1s` | The smallest `TIMESTAMP INTERVAL` a source can set. |
| `max_timestamp_interval` | `1s` | The largest `TIMESTAMP INTERVAL` a source can set. |

## Before you begin

The tick rate parameters can only be changed by the `mz_system` role. In
Materialize Cloud, [contact support](https://materialize.com/support/) to change
them. In Self-Managed Materialize, set them in your [system parameters
ConfigMap](/self-managed-deployments/configuration-system-parameters/), or
connect as `mz_system` and use [`ALTER SYSTEM SET`](/sql/alter-system-set/).

## Step 1. Lower the default tick rate

Set `default_timestamp_interval` to the tick rate you want. For example, to tick
every 250 milliseconds:

```mzsql
ALTER SYSTEM SET default_timestamp_interval = '250ms';
```

Or, in your system parameters ConfigMap:

```json
{
  "default_timestamp_interval": "250ms"
}
```

The new value applies as follows:

| Object | Effect |
| ------ | ------ |
| Tables | Use the new tick rate immediately. |
| Sources created after the change | Use the new tick rate. |
| Sources created before the change | Keep their existing tick rate until Materialize restarts, for example during an upgrade. To apply the new tick rate now, see [Step 2](#step-2-update-existing-sources). |

## Step 2. Update existing sources

For each existing source, set an explicit tick rate and then reset it, so that
the source picks up the new default:

```mzsql
ALTER SOURCE my_source SET (TIMESTAMP INTERVAL = '1s');
ALTER SOURCE my_source RESET (TIMESTAMP INTERVAL);
```

This adjusts the tick rate for all of the source's tables or subsources. Run
`ALTER SOURCE` on the source itself, not on a subsource.

To give a source its own tick rate instead, set its `TIMESTAMP INTERVAL`
option. The source tick rate must fall between the environment-level
`min_timestamp_interval` and `max_timestamp_interval`, which both default to
`1s`, so first lower the minimum:

```mzsql
ALTER SYSTEM SET min_timestamp_interval = '250ms';
ALTER SOURCE my_source SET (TIMESTAMP INTERVAL = '250ms');
```

Otherwise, the statement fails with an error like:

```nofmt
ERROR:  invalid timestamp interval of 250ms, must be in the range [1000ms, 1000ms]
```

## Step 3. Verify freshness with end-to-end latency

{{< note >}}
The wallclock lag relations, such as
[`mz_internal.mz_wallclock_global_lag`](/sql/system-catalog/mz_internal/#mz_wallclock_global_lag),
and the Console's freshness charts record lag in whole seconds, rounded up. They
report sub-second freshness as `1s`.
{{< /note >}}

To measure sub-second freshness, write timestamped heartbeat rows to an upstream
table that your source ingests, and [`SUBSCRIBE`](/sql/subscribe/) to them in
Materialize. For each row, the end-to-end latency is the client's clock when the
row arrives minus the timestamp stored in the row. This latency includes the
upstream commit, replication, the next tick, and delivery to the client, so it
is an upper bound on the source's freshness. The same client writes and reads
the rows, so clock skew doesn't affect the result.

Write the heartbeat upstream, not to a Materialize table. Writes to
Materialize tables become visible when the `INSERT` commits, so they don't
reflect the source's tick rate.

The following example uses a [PostgreSQL source](/ingest-data/postgres/).

1. In PostgreSQL, create a heartbeat table and add it to the publication your
   source uses:

   ```postgres
   CREATE TABLE heartbeat (id serial PRIMARY KEY, sent_at_ms bigint);
   ALTER TABLE heartbeat REPLICA IDENTITY FULL;
   ALTER PUBLICATION mz_source ADD TABLE heartbeat;
   ```

1. In Materialize, create a table for it from your source:

   ```mzsql
   CREATE TABLE heartbeat FROM SOURCE my_source (REFERENCE public.heartbeat);
   ```

1. Run a script that subscribes to `heartbeat` in Materialize, writes rows
   upstream, and reports the latency. For example, the following Python script
   uses [psycopg](https://www.psycopg.org/psycopg3/) to write one row every
   100 milliseconds:

   ```python
   import statistics, threading, time
   import psycopg

   MATERIALIZE = "<Materialize connection string>"
   UPSTREAM = "<PostgreSQL connection string>"
   N = 100

   latencies = []

   def subscribe():
       conn = psycopg.connect(MATERIALIZE, autocommit=True)
       query = "COPY (SUBSCRIBE (SELECT sent_at_ms FROM heartbeat) WITH (SNAPSHOT = false)) TO STDOUT"
       with conn.cursor().copy(query) as copy:
           for mz_timestamp, mz_diff, sent_at_ms in copy.rows():
               if mz_diff == "1":
                   latencies.append(time.time() * 1000 - int(sent_at_ms))
               if len(latencies) == N:
                   break

   reader = threading.Thread(target=subscribe)
   reader.start()
   time.sleep(1)  # let the subscription start

   writer = psycopg.connect(UPSTREAM, autocommit=True)
   for _ in range(N):
       writer.execute("INSERT INTO heartbeat (sent_at_ms) VALUES (%s)", (int(time.time() * 1000),))
       time.sleep(0.1)
   reader.join()

   print(f"end-to-end latency: p50 {statistics.median(latencies):.0f} ms, max {max(latencies):.0f} ms")
   ```

   With the source's tick rate set to `250ms`, the output looks like the
   following:

   ```nofmt
   end-to-end latency: p50 150 ms, max 273 ms
   ```

   For comparison, the same source at the default `1s` tick rate reports a
   maximum latency of about one second.

If the maximum latency is close to the old tick rate, check the source's tick
rate ([Step 2](#step-2-update-existing-sources)). If it is well above the tick
rate, see [Troubleshoot
freshness](/transform-data/freshness/troubleshoot-freshness/).
