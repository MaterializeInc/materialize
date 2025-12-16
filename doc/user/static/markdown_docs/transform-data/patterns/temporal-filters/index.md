<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)
 /  [Overview](/docs/self-managed/v25.2/transform-data/)
 /  [Patterns](/docs/self-managed/v25.2/transform-data/patterns/)

</div>

# Temporal filters (time windows)

A **temporal filter** is a query condition/predicate that uses the
[`mz_now()`](/docs/self-managed/v25.2/sql/functions/now_and_mz_now)
function to filter data based on a time-related condition. Using a
temporal filter reduces the working dataset, saving memory resources and
focusing on results that meet the condition.

In Materialize, you implement temporal filters using the
[`mz_now()`](/docs/self-managed/v25.2/sql/functions/now_and_mz_now)
function (which returns Materialize’s current virtual timestamp) in a
`WHERE` or `HAVING` clause; specifically, you compare
[`mz_now()`](/docs/self-managed/v25.2/sql/functions/now_and_mz_now) to a
numeric or timestamp column expression. As
[`mz_now()`](/docs/self-managed/v25.2/sql/functions/now_and_mz_now)
progresses (every millisecond), records for which the condition is no
longer true are retracted from the working dataset while records for
which the condition becomes true are included in the working dataset.
When using temporal filters, Materialize must be prepared to retract
updates in the near future and will need resources to maintain these
retractions.

For example, the following temporal filter reduces the working dataset
to those records whose event timestamp column (`event_ts`) is no more
than 5 minutes ago:

<div class="highlight">

``` chroma
WHERE mz_now() <= event_ts + INTERVAL '5min'
```

</div>

<div class="note">

**NOTE:** It may feel more natural to write this filter as the
equivalent `WHERE event_ts >= mz_now() - INTERVAL '5min'`. However,
there are currently no valid operators for the [`mz_timestamp`
type](/docs/self-managed/v25.2/sql/types/mz_timestamp) that would allow
this. See [`mz_now()` requirements and
restrictions](#mz_now-requirements-and-restrictions).

</div>

The following diagram shows record `B` falling out of the result set as
time moves forward:

- In the first timeline, record `B` occurred less than 5 minutes ago
  (occurred less than 5 minutes from `mz_now()`).

- In the second timeline, as `mz_now()` progresses, record `B` occurred
  more than 5 minutes from `mz_now()`.

![temporal filter
diagram](/docs/self-managed/v25.2/images/temporal-filter.svg)

## `mz_now()` requirements and restrictions

### `mz_now()` requirements

<div class="tip">

**💡 Tip:** When possible, prefer materialized views when using temporal
filter to take advantage of custom consolidation.

</div>

When creating a temporal filter using
[`mz_now()`](/docs/self-managed/v25.2/sql/functions/now_and_mz_now) in a
`WHERE` or `HAVING` clause, the clause has the following shape:

<div class="highlight">

``` chroma
mz_now() <comparison_operator> <numeric_expr | timestamp_expr>
```

</div>

- `mz_now()` must be used with one of the following comparison
  operators: `=`, `<`, `<=`, `>`, `>=`, or an operator that desugars to
  them or to a conjunction (`AND`) of them (for example,
  `BETWEEN...AND...`). That is, you cannot use date/time operations
  directly on `mz_now()` to calculate a timestamp in the past or future.
  Instead, rewrite the query expression to move the operation to the
  other side of the comparison.

- `mz_now()` can only be compared to either a
  [`numeric`](/docs/self-managed/v25.2/sql/types/numeric) expression or
  a [`timestamp`](/docs/self-managed/v25.2/sql/types/timestamp)
  expression not containing `mz_now()`.

### `mz_now()` restrictions

The [`mz_now()`](/docs/self-managed/v25.2/sql/functions/now_and_mz_now)
clause has the following restrictions:

- When used in a materialized view definition, a view definition that is
  being indexed (i.e., although you can create the view and perform
  ad-hoc query on the view, you cannot create an index on that view), or
  a `SUBSCRIBE` statement:

  - `mz_now()` clauses can only be combined using an `AND`, and

  - All top-level `WHERE` or `HAVING` conditions must be combined using
    an `AND`, even if the `mz_now()` clause is nested.

  To rewrite the query, see [Disjunction (OR)
  alternatives](http://localhost:1313/docs/transform-data/idiomatic-materialize-sql/mz_now/#disjunctions-or).

- If part of a `WHERE` clause, the `WHERE` clause cannot be an
  [aggregate `FILTER`
  expression](/docs/self-managed/v25.2/sql/functions/filters).

## Examples

These examples create real objects. After you have tried the examples,
make sure to drop these objects and spin down any resources you may have
created.

<div class="tip">

**💡 Tip:** When possible, prefer materialized views when using temporal
filter to take advantage of custom consolidation.

</div>

### Sliding window

It is common for real-time applications to be concerned with only a
recent period of time. We call this a **sliding window**. Other systems
use this term differently because they cannot achieve a continuously
sliding window.

In this case, we will filter a table to only include only records from
the last 30 seconds.

1.  First, create a table called `events` and a view of the most recent
    30 seconds of events.

    <div class="highlight">

    ``` chroma
    --Create a table of timestamped events.
    CREATE TABLE events (
        content TEXT,
        event_ts TIMESTAMP
    );

    -- Create a view of events from the last 30 seconds.
    CREATE VIEW last_30_sec AS
    SELECT event_ts, content
    FROM events
    WHERE mz_now() <= event_ts + INTERVAL '30s';
    ```

    </div>

2.  Next, subscribe to the results of the view.

    <div class="highlight">

    ``` chroma
    COPY (SUBSCRIBE (SELECT ts, content FROM last_30_sec)) TO STDOUT;
    ```

    </div>

3.  In a separate session, insert a record.

    <div class="highlight">

    ``` chroma
    INSERT INTO events VALUES ('hello', now());
    ```

    </div>

4.  Back in the first session, watch the record expire after 30 seconds.

    ```
    1686868190714   1       2023-06-15 22:29:50.711 hello
    1686868220712   -1      2023-06-15 22:29:50.711 hello
    ```

    Press `Ctrl+C` to quit the `SUBSCRIBE` when you are ready.

You can materialize the `last_30_sec` view by [recreating it as a
`MATERIALIZED VIEW`](/docs/self-managed/v25.2/sql/create-materialized-view/)
(results persisted to storage). When you do so, Materialize will keep
the results up to date with records expiring automatically according to
the temporal filter.

### Time-to-Live (TTL)

The **time to live (TTL)** pattern helps to filter rows with
user-defined expiration times. This example uses a `tasks` table with a
time to live for each task. Materialize then helps perform actions
according to each task’s expiration time.

1.  First, create a table:

    <div class="highlight">

    ``` chroma
    CREATE TABLE tasks (name TEXT, created_ts TIMESTAMP, ttl INTERVAL);
    ```

    </div>

2.  Add some tasks to track:

    <div class="highlight">

    ``` chroma
    INSERT INTO tasks VALUES ('send_email', now(), INTERVAL '5 minutes');
    INSERT INTO tasks VALUES ('time_to_eat', now(), INTERVAL '1 hour');
    INSERT INTO tasks VALUES ('security_block', now(), INTERVAL '1 day');
    ```

    </div>

3.  Create a view using a temporal filter **over the expiration time**.
    For our example, the expiration time represents the sum between the
    task’s `created_ts` and its `ttl`.

    <div class="highlight">

    ``` chroma
    CREATE MATERIALIZED VIEW tracking_tasks AS
    SELECT
      name,
      created_ts + ttl as expiration_time
    FROM tasks
    WHERE mz_now() < created_ts + ttl;
    ```

    </div>

    The moment `mz_now()` crosses the expiration time of a record, that
    record is retracted (removed) from the result set.

You can now:

- Query the remaining time for a row:

  <div class="highlight">

  ``` chroma
    SELECT expiration_time - now() AS remaining_ttl
    FROM tracking_tasks
    WHERE name = 'time_to_eat';
  ```

  </div>

- Check if a particular row is still available:

  <div class="highlight">

  ``` chroma
  SELECT true
  FROM tracking_tasks
  WHERE name = 'security_block';
  ```

  </div>

- Trigger an external process when a row expires:

  <div class="highlight">

  ``` chroma
    INSERT INTO tasks VALUES ('send_email', now(), INTERVAL '5 seconds');
    COPY( SUBSCRIBE tracking_tasks WITH (SNAPSHOT = false) ) TO STDOUT;
  ```

  </div>

  ```
  mz_timestamp | mz_diff | name       | expiration_time |
  -------------|---------|------------|-----------------|
  ...          | -1      | send_email | ...             | <-- Time to send the email!
  ```

### Periodically emit results

Suppose you want to count the number of records in each 1 minute time
window, grouped by an `id` column. You don’t care to receive every
update as it happens; instead, you would prefer Materialize to emit a
single result at the end of each window. Materialize [date
functions](/docs/self-managed/v25.2/sql/functions/#date-and-time-functions)
are helpful for use cases like this where you want to bucket records
into time windows.

The strategy for this example is to put an initial temporal filter on
the input (say, 30 days) to bound it, use the [`date_bin`
function](/docs/self-managed/v25.2/sql/functions/date-bin) to bin
records into 1 minute windows, use a second temporal filter to emit
results at the end of the window, and finally apply a third temporal
filter shorter than the first (say, 7 days) to set how long results
should persist in Materialize.

1.  First, create a table for the input records.
    <div class="highlight">

    ``` chroma
    CREATE TABLE input (id INT, event_ts TIMESTAMP);
    ```

    </div>

2.  Create a view that filters the input for the most recent 30 days and
    buckets records into 1 minute windows.
    <div class="highlight">

    ``` chroma
    CREATE VIEW
        input_recent_bucketed
        AS
            SELECT
                id,
                date_bin(
                        '1 minute',
                        event_ts,
                        '2000-01-01 00:00:00+00'
                    )
                    + INTERVAL '1 minute'
                    AS window_end
            FROM input
            WHERE mz_now() <= event_ts + INTERVAL '30 days';
    ```

    </div>

3.  Create the final output view that does the aggregation and maintains
    7 days worth of results.
    <div class="highlight">

    ``` chroma
    CREATE MATERIALIZED VIEW output
        AS
            SELECT
              id,
              count(id) AS count,
              window_end
            FROM input_recent_bucketed
            WHERE
                mz_now() >= window_end
                    AND
                mz_now() < window_end + INTERVAL '7 days'
            GROUP BY window_end, id;
    ```

    </div>

    This `WHERE` clause means “the result for a 1-minute window should
    come into effect when `mz_now()` reaches `window_end` and be removed
    7 days later”. Without the latter constraint, records in the result
    set would receive strange updates as records expire from the initial
    30 day filter on the input.

4.  Subscribe to the `output`.
    <div class="highlight">

    ``` chroma
    COPY (SUBSCRIBE (SELECT * FROM output)) TO STDOUT;
    ```

    </div>

5.  In a different session, insert some records.
    <div class="highlight">

    ``` chroma
    INSERT INTO input VALUES (1, now());
    -- wait a moment
    INSERT INTO input VALUES (1, now());
    -- wait a moment
    INSERT INTO input VALUES (1, now());
    -- wait a moment
    INSERT INTO input VALUES (2, now());
    ```

    </div>

6.  Back at the `SUBSCRIBE`, wait about a minute for your final
    aggregation result to show up the moment the 1 minute window ends.

    ```
     mz_timestamp | mz_diff |  id   | count |      window_end
    --------------|---------|-------|-------|----------------------
    1686889140000       1       1       3       2023-06-16 04:19:00
    1686889140000       1       2       1       2023-06-16 04:19:00
    ```

    If you are very patient, you will see these results retracted in 7
    days. Press `Ctrl+C` to exit the `SUBSCRIBE` when you are finished
    playing.

From here, you could create a [Kafka
sink](/docs/self-managed/v25.2/sql/create-sink/) and use Kafka Connect
to archive the historical results to a data warehouse (ignoring Kafka
tombstone records that represent retracted results).

## Late arriving events

For various reasons, it’s possible for records to arrive out of order.
For example, network connectivity issues may cause a mobile device to
emit data with a timestamp from the relatively distant past. How can you
account for late arriving data in Materialize?

Consider the temporal filter for the most recent hour’s worth of
records.

<div class="highlight">

``` chroma
WHERE mz_now() <= event_ts + INTERVAL '1hr'
```

</div>

Suppose a record with a timestamp `11:00:00` arrives “late” with a
virtual timestamp of `11:59:59` and you query this collection at a
virtual timestamp of `12:00:00`. According to the temporal filter, the
record is included for results as of virtual time `11:59:59` and
retracted just after `12:00:00`.

Let’s say another record comes in with a timestamp of `11:00:00`, but
`mz_now()` has marched forward to `12:00:01`. Unfortunately, this record
does not pass the filter and is excluded from processing altogether.

In conclusion: if you want to account for late arriving data up to some
given time duration, you must adjust your temporal filter to allow for
such records to make an appearance in the result set. This is often
referred to as a **grace period**.

## Temporal filter pushdown

All of the queries in the previous examples only return results based on
recently-added events. Materialize can “push down” filters that match
this pattern all the way down to its storage layer, skipping over old
data that’s not relevant to the query. Here are the key benefits of this
optimization:

- For ad-hoc `SELECT` queries, temporal filter pushdown can
  substantially improve query latency.
- When a materialized view is created or the cluster maintaining it
  restarts, temporal filter pushdown can substantially reduce the time
  it takes to start serving results.

The columns filtered should correlate with the insertion or update time
of the row. In the examples above, the `event_ts` value in each event
correlates with the time the event was inserted, so filters that
reference these columns should be pushed down to the storage layer.
However, the values in the `content` column are not correlated with
insertion time in any way, so filters against `content` will probably
not be pushed down to the storage layer.

Temporal filters that consist of arithmetic, date math, and comparisons
are eligible for pushdown, including all the examples in this page.
However, more complex filters might not be. You can check whether the
filters in your query can be pushed down by using [the `filter_pushdown`
option](/docs/self-managed/v25.2/sql/explain-plan/#output-modifiers) in
an `EXPLAIN` statement. For example:

<div class="highlight">

``` chroma
EXPLAIN WITH(filter_pushdown)
SELECT count(*)
FROM events
WHERE mz_now() <= event_ts + INTERVAL '30s';
----
Explained Query:
[...]
Source materialize.public.events
  filter=((mz_now() <= timestamp_to_mz_timestamp((#1 + 00:00:30))))
  pushdown=((mz_now() <= timestamp_to_mz_timestamp((#1 + 00:00:30))))
```

</div>

The filter in our query appears in the `pushdown=` list at the bottom of
the output, so the filter pushdown optimization will be able to filter
out irrelevant ranges of data in that source and make the overall query
more efficient.

Some common functions, such as casting from a string to a timestamp, can
prevent filter pushdown for a query. For similar functions that *do*
allow pushdown, see [the pushdown functions
documentation](/docs/self-managed/v25.2/sql/functions/pushdown/).

<div class="note">

**NOTE:** See the guide on [partitioning and filter
pushdown](/docs/self-managed/v25.2/transform-data/patterns/partition-by/)
for a **private preview** feature that can make the filter pushdown
optimization more predictable.

</div>

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/transform-data/patterns/temporal-filters.md"
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
