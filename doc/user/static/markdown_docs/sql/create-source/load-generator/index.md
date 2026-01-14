<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [SQL commands](/docs/sql/)  /  [CREATE
SOURCE](/docs/sql/create-source/)

</div>

# Appendix: Load generator

[`CREATE SOURCE`](/docs/sql/create-source/) connects Materialize to an
external system you want to read data from, and provides details about
how to decode and interpret that data.

Load generator sources produce synthetic data for use in demos and
performance tests.

## Syntax

<div class="highlight">

``` chroma
CREATE SOURCE [IF NOT EXISTS] <src_name>
[IN CLUSTER <cluster_name>]
FROM LOAD GENERATOR <generator_type> [
  (
    [TICK INTERVAL <tick_interval>]
    [, AS OF <tick>]
    [, UP TO <tick>]
    [, SCALE FACTOR <scale_factor>]
    [, MAX CARDINALITY <max_cardinality>]
    [, KEYS <keys>]
    [, SNAPSHOT ROUNDS <snapshot_rounds>]
    [, TRANSACTIONAL SNAPSHOT <transactional_snapshot>]
    [, VALUE SIZE <value_size>]
    [, SEED <seed>]
    [, PARTITIONS <partitions>]
    [, BATCH SIZE <batch_size>]
  )
]
FOR ALL TABLES
[EXPOSE PROGRESS AS <progress_subsource_name>]
[WITH (RETAIN HISTORY FOR <retention_period>)]
```

</div>

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Syntax element</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>&lt;src_name&gt;</code></td>
<td>The name for the source.</td>
</tr>
<tr>
<td><strong>IF NOT EXISTS</strong></td>
<td>Optional. If specified, do not throw an error if a source with the
same name already exists. Instead, issue a notice and skip the source
creation.</td>
</tr>
<tr>
<td><strong>IN CLUSTER</strong> <code>&lt;cluster_name&gt;</code></td>
<td>Optional. The <a href="/docs/sql/create-cluster">cluster</a> to
maintain this source.</td>
</tr>
<tr>
<td><strong>FROM LOAD GENERATOR</strong>
<code>&lt;generator_type&gt;</code></td>
<td><p>The type of load generator to use. Valid generator types:</p>
<table>
<thead>
<tr>
<th>Generator</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>AUCTION</code></td>
<td>Use the <a href="#auction">auction</a> load generator.</td>
</tr>
<tr>
<td><code>MARKETING</code></td>
<td>Use the <a href="#marketing">marketing</a> load generator.</td>
</tr>
<tr>
<td><code>TPCH</code></td>
<td>Use the <a href="#tpch">tpch</a> load generator.</td>
</tr>
<tr>
<td><code>KEY VALUE</code></td>
<td>Use the key-value load generator.</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><strong>TICK INTERVAL</strong>
<code>&lt;tick_interval&gt;</code></td>
<td>Optional. The interval at which the next datum should be emitted.
Defaults to one second.</td>
</tr>
<tr>
<td><strong>AS OF</strong> <code>&lt;tick&gt;</code></td>
<td>Optional.
<p>The tick at which to start producing data. Defaults to 0.</p></td>
</tr>
<tr>
<td><strong>UP TO</strong> <code>&lt;tick&gt;</code></td>
<td>Optional.
<p>The tick before which to stop producing data. Defaults to
infinite.</p></td>
</tr>
<tr>
<td><strong>SCALE FACTOR</strong> <code>&lt;scale_factor&gt;</code></td>
<td>Optional. The scale factor for the <code>TPCH</code> generator.
Defaults to <code>0.01</code> (~ 10MB).</td>
</tr>
<tr>
<td><strong>MAX CARDINALITY</strong>
<code>&lt;max_cardinality&gt;</code></td>
<td>Optional. The maximum cardinality for the generator.</td>
</tr>
<tr>
<td><strong>KEYS</strong> <code>&lt;keys&gt;</code></td>
<td>Optional. The number of keys for the generator.</td>
</tr>
<tr>
<td><strong>SNAPSHOT ROUNDS</strong>
<code>&lt;snapshot_rounds&gt;</code></td>
<td>Optional. The number of snapshot rounds for the generator.</td>
</tr>
<tr>
<td><strong>TRANSACTIONAL SNAPSHOT</strong>
<code>&lt;transactional_snapshot&gt;</code></td>
<td>Optional. Whether to use transactional snapshots.</td>
</tr>
<tr>
<td><strong>VALUE SIZE</strong> <code>&lt;value_size&gt;</code></td>
<td>Optional. The size of values for the generator.</td>
</tr>
<tr>
<td><strong>SEED</strong> <code>&lt;seed&gt;</code></td>
<td>Optional. The seed for random number generation.</td>
</tr>
<tr>
<td><strong>PARTITIONS</strong> <code>&lt;partitions&gt;</code></td>
<td>Optional. The number of partitions for the generator.</td>
</tr>
<tr>
<td><strong>BATCH SIZE</strong> <code>&lt;batch_size&gt;</code></td>
<td>Optional. The batch size for the generator.</td>
</tr>
<tr>
<td><strong>FOR ALL TABLES</strong></td>
<td>Creates subsources for all tables in the load generator.</td>
</tr>
<tr>
<td><strong>EXPOSE PROGRESS AS</strong>
<code>&lt;progress_subsource_name&gt;</code></td>
<td>Optional. The name of the progress subsource for the source. If this
is not specified, the subsource will be named
<code>&lt;src_name&gt;_progress</code>. For more information, see <a
href="#monitoring-source-progress">Monitoring source progress</a>.</td>
</tr>
<tr>
<td><strong>WITH</strong> (<code>&lt;with_option&gt;</code> [, …])</td>
<td><p>Optional. The following <code>&lt;with_option&gt;</code>s are
supported:</p>
<table>
<thead>
<tr>
<th>Option</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>RETAIN HISTORY FOR &lt;retention_period&gt;</code></td>
<td><em><strong>Private preview.</strong> This option has known
performance or stability issues and is under active development.</em>
Duration for which Materialize retains historical data, which is useful
to implement <a
href="/docs/transform-data/patterns/durable-subscriptions/#history-retention-period">durable
subscriptions</a>. Accepts positive <a
href="/docs/sql/types/interval/">interval</a> values (e.g.
<code>'1hr'</code>). Default: <code>1s</code>.</td>
</tr>
</tbody>
</table></td>
</tr>
</tbody>
</table>

## Description

Materialize has several built-in load generators, which provide a quick
way to get up and running with no external dependencies before plugging
in your own data sources. If you would like to see an additional load
generator, please submit a [feature
request](https://github.com/MaterializeInc/materialize/discussions/new?category=feature-requests).

### Auction

The auction load generator simulates an auction house, where users are
bidding on an ongoing series of auctions. The auction source will be
automatically demuxed into multiple subsources when the `CREATE SOURCE`
command is executed. This will create the following subsources:

- `organizations` describes the organizations known to the auction
  house.

  | Field | Type | Description |
  |----|----|----|
  | id | [`bigint`](/docs/sql/types/bigint) | A unique identifier for the organization. |
  | name | [`text`](/docs/sql/types/text) | The organization’s name. |

- `users` describes the users that belong to each organization.

  | Field | Type | Description |
  |----|----|----|
  | `id` | [`bigint`](/docs/sql/types/bigint) | A unique identifier for the user. |
  | `org_id` | [`bigint`](/docs/sql/types/bigint) | The identifier of the organization to which the user belongs. References `organizations.id`. |
  | `name` | [`text`](/docs/sql/types/text) | The user’s name. |

- `accounts` describes the account associated with each organization.

  | Field | Type | Description |
  |----|----|----|
  | `id` | [`bigint`](/docs/sql/types/bigint) | A unique identifier for the account. |
  | `org_id` | [`bigint`](/docs/sql/types/bigint) | The identifier of the organization to which the account belongs. References `organizations.id`. |
  | `balance` | [`bigint`](/docs/sql/types/bigint) | The balance of the account in dollars. |

- `auctions` describes all past and ongoing auctions.

  | Field | Type | Description |
  |----|----|----|
  | `id` | [`bigint`](/docs/sql/types/bigint) | A unique identifier for the auction. |
  | `seller` | [`bigint`](/docs/sql/types/bigint) | The identifier of the user selling the item. References `users.id`. |
  | `item` | [`text`](/docs/sql/types/text) | The name of the item being sold. |
  | `end_time` | [`timestamp with time zone`](/docs/sql/types/timestamp) | The time at which the auction closes. |

- `bids` describes the bids placed in each auction.

  | Field | Type | Description |
  |----|----|----|
  | `id` | [`bigint`](/docs/sql/types/bigint) | A unique identifier for the bid. |
  | `buyer` | [`bigint`](/docs/sql/types/bigint) | The identifier vof the user placing the bid. References `users.id`. |
  | `auction_id` | [`bigint`](/docs/sql/types/bigint) | The identifier of the auction in which the bid is placed. References `auctions.id`. |
  | `amount` | [`bigint`](/docs/sql/types/bigint) | The bid amount in dollars. |
  | `bid_time` | [`timestamp with time zone`](/docs/sql/types/timestamp) | The time at which the bid was placed. |

The organizations, users, and accounts are fixed at the time the source
is created. Each tick interval, either a new auction is started, or a
new bid is placed in the currently ongoing auction.

### Marketing

The marketing load generator simulates a marketing organization that is
using a machine learning model to send coupons to potential leads. The
marketing source will be automatically demuxed into multiple subsources
when the `CREATE SOURCE` command is executed. This will create the
following subsources:

- `customers` describes the customers that the marketing team may
  target.

  | Field | Type | Description |
  |----|----|----|
  | `id` | [`bigint`](/docs/sql/types/bigint) | A unique identifier for the customer. |
  | `email` | [`text`](/docs/sql/types/text) | The customer’s email. |
  | `income` | [`bigint`](/docs/sql/types/bigint) | The customer’s income in pennies. |

- `impressions` describes online ads that have been seen by a customer.

  | Field | Type | Description |
  |----|----|----|
  | `id` | [`bigint`](/docs/sql/types/bigint) | A unique identifier for the impression. |
  | `customer_id` | [`bigint`](/docs/sql/types/bigint) | The identifier of the customer that saw the ad. References `customers.id`. |
  | `impression_time` | [`timestamp with time zone`](/docs/sql/types/timestamp) | The time at which the ad was seen. |

- `clicks` describes clicks of ads.

  | Field | Type | Description |
  |----|----|----|
  | `impression_id` | [`bigint`](/docs/sql/types/bigint) | The identifier of the impression that was clicked. References `impressions.id`. |
  | `click_time` | [`timestamp with time zone`](/docs/sql/types/timestamp) | The time at which the impression was clicked. |

- `leads` describes a potential lead for a purchase.

  | Field | Type | Description |
  |----|----|----|
  | `id` | [`bigint`](/docs/sql/types/bigint) | A unique identifier for the lead. |
  | `customer_id` | [`bigint`](/docs/sql/types/bigint) | The identifier of the customer we’d like to convert. References `customers.id`. |
  | `created_at` | [`timestamp with time zone`](/docs/sql/types/timestamp) | The time at which the lead was created. |
  | `converted_at` | [`timestamp with time zone`](/docs/sql/types/timestamp) | The time at which the lead was converted. |
  | `conversion_amount` | [`bigint`](/docs/sql/types/bigint) | The amount the lead converted for in pennies. |

- `coupons` describes coupons given to leads.

  | Field | Type | Description |
  |----|----|----|
  | `id` | [`bigint`](/docs/sql/types/bigint) | A unique identifier for the coupon. |
  | `lead_id` | [`bigint`](/docs/sql/types/bigint) | The identifier of the lead we’re attempting to convert. References `leads.id`. |
  | `created_at` | [`timestamp with time zone`](/docs/sql/types/timestamp) | The time at which the coupon was created. |
  | `amount` | [`bigint`](/docs/sql/types/bigint) | The amount the coupon is for in pennies. |

- `conversion_predictions` describes the predictions made by a highly
  sophisticated machine learning model.

  | Field | Type | Description |
  |----|----|----|
  | `lead_id` | [`bigint`](/docs/sql/types/bigint) | The identifier of the lead we’re attempting to convert. References `leads.id`. |
  | `experiment_bucket` | [`text`](/docs/sql/types/text) | Whether the lead is a control or experiment. |
  | `created_at` | [`timestamp with time zone`](/docs/sql/types/timestamp) | The time at which the prediction was made. |
  | `score` | [`numeric`](/docs/sql/types/numeric) | The predicted likelihood the lead will convert. |

### TPCH

The TPCH load generator implements the [TPC-H benchmark
specification](https://www.tpc.org/tpch/default5.asp). The TPCH source
must be used with `FOR ALL TABLES`, which will create the standard TPCH
relations. If `TICK INTERVAL` is specified, after the initial data load,
an order and its lineitems will be changed at this interval. If not
specified, the dataset will not change over time.

### Monitoring source progress

By default, load generator sources expose progress metadata as a
subsource that you can use to monitor source **ingestion progress**. The
name of the progress subsource can be specified when creating a source
using the `EXPOSE PROGRESS AS` clause; otherwise, it will be named
`<src_name>_progress`.

The following metadata is available for each source as a progress
subsource:

| Field | Type | Meaning |
|----|----|----|
| `offset` | [`uint8`](/docs/sql/types/uint/#uint8-info) | The minimum offset for which updates to this sources are still undetermined. |

And can be queried using:

<div class="highlight">

``` chroma
SELECT "offset"
FROM <src_name>_progress;
```

</div>

As long as the offset continues increasing, Materialize is generating
data. For more details on monitoring source ingestion progress and
debugging related issues, see
[Troubleshooting](/docs/ops/troubleshooting/).

## Examples

### Creating an auction load generator

To create a load generator source that simulates an auction house and
emits new data every second:

<div class="highlight">

``` chroma
CREATE SOURCE auction_house
  FROM LOAD GENERATOR AUCTION
  (TICK INTERVAL '1s')
  FOR ALL TABLES;
```

</div>

To display the created subsources:

<div class="highlight">

``` chroma
SHOW SOURCES;
```

</div>

```
          name          |      type
------------------------+----------------
 accounts               | subsource
 auction_house          | load-generator
 auction_house_progress | progress
 auctions               | subsource
 bids                   | subsource
 organizations          | subsource
 users                  | subsource
```

To examine the simulated bids:

<div class="highlight">

``` chroma
SELECT * from bids;
```

</div>

```
 id | buyer | auction_id | amount |          bid_time
----+-------+------------+--------+----------------------------
 10 |  3844 |          1 |     59 | 2022-09-16 23:24:07.332+00
 11 |  1861 |          1 |     40 | 2022-09-16 23:24:08.332+00
 12 |  3338 |          1 |     97 | 2022-09-16 23:24:09.332+00
```

### Creating a marketing load generator

To create a load generator source that simulates an online marketing
campaign:

<div class="highlight">

``` chroma
CREATE SOURCE marketing
  FROM LOAD GENERATOR MARKETING
  FOR ALL TABLES;
```

</div>

To display the created subsources:

<div class="highlight">

``` chroma
SHOW SOURCES;
```

</div>

```
          name          |      type
------------------------+---------------
 clicks                 | subsource
 conversion_predictions | subsource
 coupons                | subsource
 customers              | subsource
 impressions            | subsource
 leads                  | subsource
 marketing              | load-generator
 marketing_progress     | progress
```

To find all impressions and clicks associated with a campaign over the
last 30 days:

<div class="highlight">

``` chroma
WITH
    click_rollup AS
    (
        SELECT impression_id AS id, count(*) AS clicks
        FROM clicks
        WHERE click_time - INTERVAL '30' DAY <= mz_now()
        GROUP BY impression_id
    ),
    impression_rollup AS
    (
        SELECT id, campaign_id, count(*) AS impressions
        FROM impressions
        WHERE impression_time - INTERVAL '30' DAY <= mz_now()
        GROUP BY id, campaign_id
    )
SELECT campaign_id, sum(impressions) AS impressions, sum(clicks) AS clicks
FROM impression_rollup LEFT JOIN click_rollup USING(id)
GROUP BY campaign_id;
```

</div>

```
 campaign_id | impressions | clicks
-------------+-------------+--------
           0 |         350 |     33
           1 |         325 |     28
           2 |         319 |     24
           3 |         315 |     38
           4 |         305 |     28
           5 |         354 |     31
           6 |         346 |     25
           7 |         337 |     36
           8 |         329 |     38
           9 |         305 |     24
          10 |         345 |     27
          11 |         323 |     30
          12 |         320 |     29
          13 |         331 |     27
          14 |         310 |     22
          15 |         324 |     28
          16 |         315 |     32
          17 |         329 |     36
          18 |         329 |     28
```

### Creating a TPCH load generator

To create the load generator source and its associated subsources:

<div class="highlight">

``` chroma
CREATE SOURCE tpch
  FROM LOAD GENERATOR TPCH (SCALE FACTOR 1)
  FOR ALL TABLES;
```

</div>

To display the created subsources:

<div class="highlight">

``` chroma
SHOW SOURCES;
```

</div>

```
      name     |      type
---------------+---------------
 tpch          | load-generator
 tpch_progress | progress
 supplier      | subsource
 region        | subsource
 partsupp      | subsource
 part          | subsource
 orders        | subsource
 nation        | subsource
 lineitem      | subsource
 customer      | subsource
```

To run the Pricing Summary Report Query (Q1), which reports the amount
of billed, shipped, and returned items:

<div class="highlight">

``` chroma
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
FROM
    lineitem
WHERE
    l_shipdate <= date '1998-12-01' - interval '90' day
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;
```

</div>

```
 l_returnflag | l_linestatus | sum_qty  | sum_base_price | sum_disc_price  |    sum_charge     |      avg_qty       |     avg_price      |      avg_disc       | count_order
--------------+--------------+----------+----------------+-----------------+-------------------+--------------------+--------------------+---------------------+-------------
 A            | F            | 37772997 |    56604341792 |  54338346989.17 |  57053313118.2657 | 25.490380624798817 | 38198.351517998075 | 0.04003729114831228 |     1481853
 N            | F            |   986796 |     1477585066 |   1418531782.89 |   1489171757.0798 | 25.463731840115603 |  38128.27564317601 | 0.04007431682708436 |       38753
 N            | O            | 74281600 |   111337230039 | 106883023012.04 | 112227399730.9018 |  25.49430183051871 | 38212.221432873834 | 0.03999775539657235 |     2913655
 R            | F            | 37770949 |    56610551077 |   54347734573.7 |  57066196254.4557 | 25.496431466814634 |  38213.68205054471 | 0.03997848687172654 |     1481421
```

## Related pages

- [`CREATE SOURCE`](../)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/create-source/load-generator.md"
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

© 2026 Materialize Inc.

</div>
