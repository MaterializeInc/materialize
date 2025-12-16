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

# Durable subscriptions

[Subscriptions](/docs/self-managed/v25.2/sql/subscribe/) allow you to
stream changing results from Materialize to an external application
programatically. Like any connection over the network, subscriptions
might get disrupted for both expected and unexpected reasons. In such
cases, it can be useful to have a mechanism to gracefully recover data
processing.

To avoid the need for re-processing data that was already sent to your
external application following a connection disruption, you can:

- Adjust the [history retention period](#history-retention-period) for
  the objects that a subscription depends on, and

- [Access past versions of this
  data](#enabling-durable-subscriptions-in-your-application) at specific
  points in time to pick up data processing where you left off.

## History retention period

<div class="private-preview">

**PREVIEW** This feature is in **[private
preview](https://materialize.com/preview-terms/)**. It is under active
development and may have stability or performance issues. It isn't
subject to our backwards compatibility guarantees.  
  
To enable this feature in your Materialize region, [contact our
team](https://materialize.com/docs/support/).

</div>

By default, all user-defined sources, tables, materialized views, and
indexes keep track of the most recent version of their underlying data.
To gracefully recover from connection disruptions and enable lossless,
*durable subscriptions*, you can configure the sources, tables, and
materialized views that the subscription depends on to **retain
history**.

<div class="important">

**! Important:** Configuring indexes to retain history is not
recommended. Instead, consider creating a materialized view for your
subscription query and configuring the history retention period on that
view.

</div>

To configure the history retention period for sources, tables and
materialized views, use the `RETAIN HISTORY` option in its `CREATE`
statement. This value can also be adjusted at any time using the
object-specific `ALTER` statement.

### Semantics

#### Increasing the history retention period

When you increase the history retention period for an object, both the
existing historical data and any subsequently produced historical data
are retained for the specified time period.

**For sources, tables and materialized views:** increasing the history
retention period will not restore older historical data that was already
outside the previous history retention period before the change.

Configuring indexes to retain history is not recommended. Instead,
consider creating a materialized view for your subscription query and
configuring the history retention period on that view.

See also [Considerations](#considerations).

#### Decreasing the history retention period

When you decrease the history retention period for an object:

- Newly produced historical data is retained for the new, shorter
  history retention period.

- Historical data outside the new, shorter history retention period is
  no longer retained. If you subsequently increase the history retention
  period again, the older historical data may already be unavailable.

See also [Considerations](#considerations).

### Set history retention period

<div class="important">

**! Important:** Setting the history retention period for an object will
lead to increased resource utilization. Moreover, for indexes, setting
history retention period is not recommended. Instead, consider creating
a materialized view for your subscription query and configuring the
history retention period on that view. See
[Considerations](#considerations).

</div>

To set the history retention period for
[sources](/docs/self-managed/v25.2/sql/create-source/),
[tables](/docs/self-managed/v25.2/sql/create-table/), and [materialized
views](/docs/self-managed/v25.2/sql/create-materialized-view/), you can
either:

- Specify the `RETAIN HISTORY` option in the `CREATE` statement. The
  `RETAIN HISTORY` option accepts positive
  [interval](/docs/self-managed/v25.2/sql/types/interval/) values (e.g.,
  `'1hr'`). For example:

  <div class="highlight">

  ``` chroma
  CREATE MATERIALIZED VIEW winning_bids
  WITH (RETAIN HISTORY FOR '1hr') AS
  SELECT auction_id,
        bid_id,
        item,
        amount
  FROM highest_bid_per_auction
  WHERE end_time < mz_now();
  ```

  </div>

- Specify the `RETAIN HISTORY` option in the `ALTER` statement. The
  `RETAIN HISTORY` option accepts positive
  [interval](/docs/self-managed/v25.2/sql/types/interval/) values (e.g.,
  `'1hr'`). For example:

  <div class="highlight">

  ``` chroma
  ALTER MATERIALIZED VIEW winning_bids SET (RETAIN HISTORY FOR '2hr');
  ```

  </div>

### View history retention period for an object

To see what history retention period has been configured for an object,
look up the object in the
[`mz_internal.mz_history_retention_strategies`](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_history_retention_strategies)
catalog table. For example:

<div class="highlight">

``` chroma
SELECT
    d.name AS database_name,
    s.name AS schema_name,
    mv.name,
    hrs.strategy,
    hrs.value
FROM
    mz_catalog.mz_materialized_views AS mv
        LEFT JOIN mz_schemas AS s ON mv.schema_id = s.id
        LEFT JOIN mz_databases AS d ON s.database_id = d.id
        LEFT JOIN mz_internal.mz_history_retention_strategies AS hrs ON mv.id = hrs.id
WHERE mv.name = 'winning_bids';
```

</div>

If set, the returning result includes the value (in milliseconds) of the
history retention period:

```

 database_name | schema_name |     name     | strategy |  value
---------------+-------------+--------------+----------+---------
 materialize   | public      | winning_bids | FOR      | 7200000
```

### Unset/reset history retention period

To disable history retention, reset the history retention period; i.e.,
specify the `RESET (RETAIN HISTORY)` option in the `ALTER` statement.
For example:

<div class="highlight">

``` chroma
ALTER MATERIALIZED VIEW winning_bids RESET (RETAIN HISTORY);
```

</div>

### Considerations

#### Resource utilization

Increasing the history retention period for an object will lead to
increased resource utilization in Materialize.

**For sources, tables and materialized views:** Increasing the history
retention period for these objects increases the amount of historical
data that is retained in the storage layer. You can expect storage
resource utilization to increase, which may incur additional costs.

**For indexes:** Configuring indexes to retain history is not
recommended. Instead, consider creating a materialized view for your
subscription query and configuring the history retention period on that
view.

#### Best practices

- Because of the increased storage costs and processing time for the
  additional historical data, consider configuring history retention
  period on the object directly powering the subscription, rather than
  all the way through the dependency chain from the source to the
  materialized view.

- Configuring indexes to retain history is not recommended. Instead,
  consider creating a materialized view for your subscription query and
  configuring the history retention period on that view.

#### Clean-up

The history retention period represents the minimum amount of historical
data guaranteed to be retained by Materialize. History clean-up is
processed in the background, so older history may be accessible for the
period of time between when it falls outside the retention period and
when it is cleaned up.

## Enabling durable subscriptions in your application

1.  In Materialize, configure the history retention period for the
    object(s) queried in the `SUBSCRIBE`. Choose a duration you expect
    will allow you to recover in case of connection drops. One hour
    (`1h`) is a good place to start, though you should be mindful of the
    [impact](#considerations) of increasing an object’s history
    retention period.

2.  In order to restart your application without losing or
    re-snapshotting data after a connection drop, you need to store the
    latest timestamp processed for the subscription (either in
    Materialize, or elsewhere in your application state). This will
    allow you to resume using the retained history upstream.

3.  The first time you start the subscription, run the following
    continuous query against Materialize in your application code:

    <div class="highlight">

    ``` chroma
    SUBSCRIBE (<your query>) WITH (PROGRESS, SNAPSHOT true);
    ```

    </div>

    If you do not need a full snapshot to bootstrap your application,
    change this to `SNAPSHOT false`.

4.  As results come in continuously, buffer the latest results in memory
    until you receive a
    [progress](/docs/self-managed/v25.2/sql/subscribe#progress) message.
    At that point, the data up until the progress message is complete,
    so you can:

    1.  Process all the buffered data in your application.
    2.  Persist the `mz_timestamp` of the progress message.

5.  To resume the subscription in subsequent restarts, use the following
    continuous query against Materialize in your application code:

    <div class="highlight">

    ``` chroma
    SUBSCRIBE (<your query>) WITH (PROGRESS, SNAPSHOT false) AS OF <last_progress_mz_timestamp>;
    ```

    </div>

    In a similar way, as results come in continuously, buffer the latest
    results in memory until you receive a
    [progress](/docs/self-managed/v25.2/sql/subscribe#progress) message.
    At that point, the data up until the progress message is complete,
    so you can:

    1.  Process all the buffered data in your application.
    2.  Persist the `mz_timestamp` of the progress message.

    You can tweak the flush interval at which you durably record the
    latest progress timestamp, if every progress message is too
    frequent.

### Note about idempotency

The guidance above recommends you buffer data in memory until receiving
a progress message, and then persist the data and progress message
`mz_timestamp` at the same time. This is to ensure data is processed
**exactly once**.

In the case that your application crashes and you need to resume your
subscription using the persisted progress message `mz_timestamp`:

- If you were processing data in your application before persisting the
  subsequent progress message’s `mz_timestamp`: you may end up
  processing duplicate data.
- If you were persisting the progress message’s `mz_timestamp` before
  processing all the buffered data from before that progress message:
  you may end up dropping some data.

As a result, to guarantee that the data processing occurs only once
after your application crashes, you must write the progress message
`mz_timestamp` and all buffered data **together in a single
transaction**.

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/transform-data/patterns/durable-subscriptions.md"
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
