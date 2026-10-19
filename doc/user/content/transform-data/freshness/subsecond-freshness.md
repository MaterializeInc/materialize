---
title: "Fine tune for sub-second freshness"
description: "How to lower the tick rate to get sub-second freshness, and how to verify it with SUBSCRIBE."
menu:
  main:
    name: "Fine tune for sub-second freshness"
    identifier: subsecond-freshness
    parent: freshness
    weight: 10
---

To get sub-second [freshness](/fundamentals/concepts/reaction-time/#freshness),
lower the tick rate of your sources and tables, then verify the result with
[`SUBSCRIBE`](/sql/subscribe/).

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
| Sources created before the change | Keep their existing tick rate. See [Step 2](#step-2-update-existing-sources). |

## Step 2. Update existing sources

For each existing source, reset its tick rate to the new default:

```mzsql
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
  match the tick rate you set.
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
started = False
deadline = time.time() + 10
query = "COPY (SUBSCRIBE (SELECT count(*) FROM my_mv) WITH (PROGRESS, SNAPSHOT = false)) TO STDOUT"
with conn.cursor().copy(query) as copy:
    for mz_timestamp, mz_progressed, *_ in copy.rows():
        now_ms = time.time() * 1000
        if mz_progressed == "t":
            # The first progress row is the subscription's starting frontier, not a tick.
            if started:
                frontiers.append(int(mz_timestamp))
                delays.append(now_ms - int(mz_timestamp))
            started = True
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
delay at receipt: p50 13 ms, max 27 ms
worst-case freshness: 352 ms
```

If the tick interval doesn't match the value you set, check the source's tick
rate ([Step 2](#step-2-update-existing-sources)). If the tick interval is
right but the delay is high, see [Troubleshoot
freshness](/transform-data/freshness/troubleshoot-freshness/).
