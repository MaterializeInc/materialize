---
title: "Fine tune for sub-second freshness"
description: "How to lower the timestamp interval to get sub-second freshness, and how to verify it with SUBSCRIBE."
menu:
  main:
    name: "Fine tune for sub-second freshness"
    identifier: subsecond-freshness
    parent: freshness
    weight: 10
---

Materialize assigns timestamps to data from sources and tables at a fixed
interval, the **timestamp interval** (also called the tick rate). The default
is `1s`. A change becomes visible to queries only after the next tick, so the
timestamp interval sets a floor on
[freshness](/fundamentals/concepts/reaction-time/#freshness). To get sub-second
freshness, lower the timestamp interval, then verify the result with
[`SUBSCRIBE`](/sql/subscribe/).

## Before you begin

- The timestamp interval parameters can only be changed by the `mz_system`
  role. In Materialize Cloud, [contact support](https://materialize.com/support/) to change them. In
  Self-Managed Materialize, set them in your [system parameters
  ConfigMap](/self-managed-deployments/configuration-system-parameters/), or
  connect as `mz_system` and use [`ALTER SYSTEM
  SET`](/sql/alter-system-set/).
- Each tick advances the frontier of every source and table, which writes to
  Materialize's storage layer and its metadata store. A shorter interval
  increases that write load, especially in environments with many sources and
  tables. Test the interval you choose with your workload before you roll it
  out.

## Step 1. Lower the default timestamp interval

Set `default_timestamp_interval` to the interval you want. For example, to tick
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
| Tables | Use the new interval immediately. |
| Sources created after the change | Use the new interval. |
| Sources created before the change | Keep their existing interval. See [Step 2](#step-2-update-existing-sources). |

Views, indexes, and materialized views advance at the rate of the sources and
tables they depend on.

## Step 2. Update existing sources

The `TIMESTAMP INTERVAL` option of [`CREATE SOURCE`](/sql/create-source/) and
[`ALTER SOURCE`](/sql/alter-source/) must fall between `min_timestamp_interval`
and `max_timestamp_interval`. Both default to `1s`, so first lower the minimum:

```mzsql
ALTER SYSTEM SET min_timestamp_interval = '250ms';
```

Otherwise, setting a smaller interval on a source fails with an error like:

```nofmt
ERROR:  invalid timestamp interval of 250ms, must be in the range [1000ms, 1000ms]
```

Then, for each existing source, either reset the interval to the new system
default or set it explicitly:

```mzsql
-- Use the current value of default_timestamp_interval
ALTER SOURCE my_source RESET (TIMESTAMP INTERVAL);

-- Or set an interval for this source only
ALTER SOURCE my_source SET (TIMESTAMP INTERVAL = '250ms');
```

## Step 3. Verify freshness with SUBSCRIBE

{{< note >}}
The wallclock lag relations, such as
[`mz_internal.mz_wallclock_global_lag`](/sql/system-catalog/mz_internal/#mz_wallclock_global_lag),
and the Console's freshness charts record lag in whole seconds, rounded up. They
report sub-second freshness as `1s`.
{{< /note >}}

To measure sub-second freshness, subscribe to the object you query with the
[`PROGRESS`](/sql/subscribe/#progress) option. Each progress row reports, in
`mz_timestamp`, the object's new write frontier as milliseconds since the Unix
epoch. All changes before that timestamp are visible. Comparing progress
timestamps with your client's wall clock shows:

- **Tick rate:** the gap between consecutive progress timestamps. It should
  match your timestamp interval.
- **Freshness:** the wall-clock time minus the latest progress timestamp.
  Freshness is worst just before the next progress row arrives, at about the
  tick interval plus the delay you observe at receipt.

For example, the following Python script uses
[psycopg](https://www.psycopg.org/psycopg3/) to subscribe to a materialized view
named `my_mv` for 10 seconds and prints these measurements:

```python
import statistics, time
import psycopg

conn = psycopg.connect("<connection string>", autocommit=True)
frontiers, delays = [], []
deadline = time.time() + 10
query = "COPY (SUBSCRIBE (SELECT count(*) FROM my_mv) WITH (PROGRESS, SNAPSHOT = false)) TO STDOUT"
with conn.cursor().copy(query) as copy:
    for mz_timestamp, mz_progressed, *_ in copy.rows():
        now_ms = time.time() * 1000
        if mz_progressed == "t":
            frontiers.append(int(mz_timestamp))
            delays.append(now_ms - int(mz_timestamp))
        if time.time() > deadline:
            break

gaps = [b - a for a, b in zip(frontiers, frontiers[1:])]
print(f"tick interval: {statistics.median(gaps):.0f} ms")
print(f"delay at receipt: p50 {statistics.median(delays):.0f} ms, max {max(delays):.0f} ms")
print(f"worst-case freshness: {max(gaps) + max(delays):.0f} ms")
```

With `default_timestamp_interval` set to `250ms`, the output looks like the
following:

```nofmt
tick interval: 250 ms
delay at receipt: p50 9 ms, max 66 ms
worst-case freshness: 317 ms
```

If the tick interval doesn't match the value you set, check the source's
interval ([Step 2](#step-2-update-existing-sources)). If the tick interval is
right but the delay is high, see [Troubleshoot
freshness](/transform-data/freshness/troubleshoot-freshness/).
