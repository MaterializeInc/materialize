---
title: "CREATE SOURCE: Load generator"
description: "Using Materialize's built-in load generators"
pagerank: 50
menu:
  main:
    parent: 'create-source'
    identifier: load-generator
    name: Load generator
    weight: 40
---

{{% create-source/intro %}}
Load generator sources produce synthetic data for use in demos and performance
tests.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-load-generator.svg" >}}

#### `load_generator_option`

{{< diagram "load-generator-option.svg" >}}

Field | Use
------|-----
_src_name_  | The name for the source.
**IN CLUSTER** _cluster_name_ | The [cluster](/sql/create-cluster) to maintain this source. If not specified, the `SIZE` option must be specified.
**COUNTER** | Use the [counter](#counter) load generator.
**AUCTION** | Use the [auction](#auction) load generator.
**TPCH**    | Use the [tpch](#tpch) load generator.
**IF NOT EXISTS**  | Do nothing (except issuing a notice) if a source with the same name already exists.
**TICK INTERVAL**  | The interval at which the next datum should be emitted. Defaults to one second.
**SCALE FACTOR**   | The scale factor for the `TPCH` generator. Defaults to `0.01` (~ 10MB).
**MAX CARDINALITY** | Valid for the `COUNTER` generator. Causes the generator to delete old values to keep the collection at most a given size. Defaults to unlimited.
**FOR ALL TABLES** | Creates subsources for all tables in the load generator.
**FOR TABLES (** _table_list_ **)** | Creates subsources for specific tables in the load generator.
**EXPOSE PROGRESS AS** _progress_subsource_name_ | The name of the progress subsource for the source. If this is not specified, the subsource will be named `<src_name>_progress`. For more information, see [Monitoring source progress](#monitoring-source-progress).

### `WITH` options

Field                                | Value     | Description
-------------------------------------|-----------|-------------------------------------
`SIZE`                               | `text`    | The [size](../#sizing-a-source) for the source. Accepts values: `3xsmall`, `2xsmall`, `xsmall`, `small`, `medium`, `large`, `xlarge`. Required if the `IN CLUSTER` option is not specified.

## Description

Materialize has several built-in load generators, which provide a quick way to
get up and running with no external dependencies before plugging in your own
data sources. If you would like to see an additional load generator, please
submit a [feature request].

### Counter

The counter load generator produces the sequence `1`, `2`, `3`, …. Each tick
interval, the next number in the sequence is emitted.

### Auction

The auction load generator simulates an auction house, where users are bidding
on an ongoing series of auctions. The auction source will be automatically demuxed
into multiple subsources when the `CREATE SOURCE` command is executed. This will
create the following subsources:

  * `organizations` describes the organizations known to the auction
    house.

    Field | Type       | Description
    ------|------------|------------
    id    | [`bigint`] | A unique identifier for the organization.
    name  | [`text`]   | The organization's name.

  * `users` describes the users that belong to each organization.

    Field     | Type       | Description
    ----------|------------|------------
    `id`      | [`bigint`] | A unique identifier for the user.
    `org_id`  | [`bigint`] | The identifier of the organization to which the user belongs. References `organizations.id`.
    `name`    | [`text`]   | The user's name.

  * `accounts` describes the account associated with each organization.

    Field     | Type       | Description
    ----------|------------|------------
    `id`      | [`bigint`] | A unique identifier for the account.
    `org_id`  | [`bigint`] | The identifier of the organization to which the account belongs. References `organizations.id`.
    `balance` | [`bigint`] | The balance of the account in dollars.

  * `auctions` describes all past and ongoing auctions.

    Field      | Type                         | Description
    -----------|------------------------------|------------
    `id`       | [`bigint`]                   | A unique identifier for the auction.
    `seller`   | [`bigint`]                   | The identifier of the user selling the item. References `users.id`.
    `item`     | [`text`]                     | The name of the item being sold.
    `end_time` | [`timestamp with time zone`] | The time at which the auction closes.

  * `bids` describes the bids placed in each auction.

    Field        | Type                         | Description
    -------------|------------------------------|------------
    `id`         | [`bigint`]                   | A unique identifier for the bid.
    `buyer`      | [`bigint`]                   | The identifier vof the user placing the bid. References `users.id`.
    `auction_id` | [`text`]                     | The identifier of the auction in which the bid is placed. References `auctions.id`.
    `amount`     | [`bigint`]                   | The bid amount in dollars.
    `bid_time`   | [`timestamp with time zone`] | The time at which the bid was placed.

The organizations, users, and accounts are fixed at the time the source
is created. Each tick interval, either a new auction is started, or a new bid
is placed in the currently ongoing auction.

### Marketing

The marketing load generator simulates a marketing organization that is using a machine learning model to send coupons to potential leads. The marketing source will be automatically demuxed
into multiple subsources when the `CREATE SOURCE` command is executed. This will
create the following subsources:

  * `customers` describes the customers that the marketing team may target.

    Field     | Type       | Description
    ----------|------------|------------
    `id`      | [`bigint`] | A unique identifier for the customer.
    `email`   | [`text`]   | The customer's email.
    `income`  | [`bigint`] | The customer's income in pennies.

  * `impressions` describes online ads that have been seen by a customer.

    Field             | Type                         | Description
    ------------------|------------------------------|------------
    `id`              | [`bigint`]                   | A unique identifier for the impression.
    `customer_id`     | [`bigint`]                   | The identifier of the customer that saw the ad. References `customers.id`.
    `impression_time` | [`timestamp with time zone`] | The time at which the ad was seen.

  * `clicks` describes clicks of ads.

    Field             | Type                         | Description
    ------------------|------------------------------|------------
    `impression_id`   | [`bigint`]                   | The identifier of the impression that was clicked. References `impressions.id`.
    `click_time`      | [`timestamp with time zone`] | The time at which the impression was clicked.

  * `leads` describes a potential lead for a purchase.

    Field               | Type                         | Description
    --------------------|------------------------------|------------
    `id`                | [`bigint`]                   | A unique identifier for the lead.
    `customer_id`       | [`bigint`]                   | The identifier of the customer we'd like to convert. References `customers.id`.
    `created_at`        | [`timestamp with time zone`] | The time at which the lead was created.
    `converted_at`      | [`timestamp with time zone`] | The time at which the lead was converted.
    `conversion_amount` | [`bigint`]                   | The amount the lead converted for in pennies.

  * `coupons` describes coupons given to leads.

    Field               | Type                         | Description
    --------------------|------------------------------|------------
    `id`                | [`bigint`]                   | A unique identifier for the coupon.
    `lead_id`           | [`bigint`]                   | The identifier of the lead we're attempting to convert. References `leads.id`.
    `created_at`        | [`timestamp with time zone`] | The time at which the coupon was created.
    `amount`            | [`bigint`]                   | The amount the coupon is for in pennies.

  * `conversion_predictions` describes the predictions made by a highly sophisticated machine learning model.

    Field               | Type                         | Description
    --------------------|------------------------------|------------
    `lead_id`           | [`bigint`]                   | The identifier of the lead we're attempting to convert. References `leads.id`.
    `experiment_bucket`| [`text`]                     | Whether the lead is a control or experiment.
    `created_at`        | [`timestamp with time zone`] | The time at which the prediction was made.
    `score`             | [`numeric`]                  | The predicted likelihood the lead will convert.

### TPCH

The TPCH load generator implements the [TPC-H benchmark specification](https://www.tpc.org/tpch/default5.asp).
The TPCH source must be used with `FOR ALL TABLES`, which will create the standard TPCH relations.
If `TICK INTERVAL` is specified, after the initial data load, an order and its lineitems will be changed at this interval.
If not specified, the dataset will not change over time.

### Monitoring source progress

By default, load generator sources expose progress metadata as a subsource that
you can use to monitor source **ingestion progress**. The name of the progress
subsource can be specified when creating a source using the `EXPOSE PROGRESS
AS` clause; otherwise, it will be named `<src_name>_progress`.

The following metadata is available for each source as a progress subsource:

Field          | Type                                     | Meaning
---------------|------------------------------------------|--------
`offset`       | [`uint8`](/sql/types/uint/#uint8-info)   | The greatest offset generated, which equates to the number of times the load generator has emitted data.

And can be queried using:

```sql
SELECT "offset"
FROM <src_name>_progress;
```

As long as the offset continues increasing, Materialize is generating data. For
more details on monitoring source ingestion progress and debugging related
issues, see [Troubleshooting](/ops/troubleshooting/).

## Examples

### Creating a counter load generator

To create a load generator source that emits the next number in the sequence every
500 milliseconds:

```sql
CREATE SOURCE counter
  FROM LOAD GENERATOR COUNTER
  (TICK INTERVAL '500ms')
  WITH (SIZE = '3xsmall');
```

To examine the counter:

```sql
SELECT * FROM counter;
```
```nofmt
 counter
---------
       1
       2
       3
```

### Creating an auction load generator

To create a load generator source that simulates an auction house and emits new data every second:

```sql
CREATE SOURCE auction_house
  FROM LOAD GENERATOR AUCTION
  (TICK INTERVAL '1s')
  FOR ALL TABLES
  WITH (SIZE = '3xsmall');
```

To display the created subsources:

```sql
SHOW SOURCES;
```
```nofmt
     name      |      type      |  size
---------------+----------------+---------
 accounts      | subsource      |
 auction_house | load-generator | 3xsmall
 auctions      | subsource      |
 bids          | subsource      |
 organizations | subsource      |
 users         | subsource      |
```

To examine the simulated bids:

```sql
SELECT * from bids;
```
```nofmt
 id | buyer | auction_id | amount |          bid_time
----+-------+------------+--------+----------------------------
 10 |  3844 |          1 |     59 | 2022-09-16 23:24:07.332+00
 11 |  1861 |          1 |     40 | 2022-09-16 23:24:08.332+00
 12 |  3338 |          1 |     97 | 2022-09-16 23:24:09.332+00
```

### Creating a TPCH load generator

To create the load generator source and its associated subsources:

```sql
CREATE SOURCE tpch
  FROM LOAD GENERATOR TPCH (SCALE FACTOR 1)
  FOR ALL TABLES
  WITH (SIZE = '3xsmall');
```

To display the created subsources:

```sql
SHOW SOURCES;
```
```nofmt
   name   |      type      |  size
----------+----------------+---------
 tpch     | load-generator | 3xsmall
 supplier | subsource      |
 region   | subsource      |
 partsupp | subsource      |
 part     | subsource      |
 orders   | subsource      |
 nation   | subsource      |
 lineitem | subsource      |
 customer | subsource      |
```

To run the Pricing Summary Report Query (Q1), which reports the amount of
billed, shipped, and returned items:

```sql
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
```nofmt
 l_returnflag | l_linestatus | sum_qty  | sum_base_price | sum_disc_price  |    sum_charge     |      avg_qty       |     avg_price      |      avg_disc       | count_order
--------------+--------------+----------+----------------+-----------------+-------------------+--------------------+--------------------+---------------------+-------------
 A            | F            | 37772997 |    56604341792 |  54338346989.17 |  57053313118.2657 | 25.490380624798817 | 38198.351517998075 | 0.04003729114831228 |     1481853
 N            | F            |   986796 |     1477585066 |   1418531782.89 |   1489171757.0798 | 25.463731840115603 |  38128.27564317601 | 0.04007431682708436 |       38753
 N            | O            | 74281600 |   111337230039 | 106883023012.04 | 112227399730.9018 |  25.49430183051871 | 38212.221432873834 | 0.03999775539657235 |     2913655
 R            | F            | 37770949 |    56610551077 |   54347734573.7 |  57066196254.4557 | 25.496431466814634 |  38213.68205054471 | 0.03997848687172654 |     1481421
```

### Sizing a source

To provision a specific amount of CPU and memory to a source on creation, use the `SIZE` option:

```sql
CREATE SOURCE auction_load
  FROM LOAD GENERATOR AUCTION
  FOR ALL TABLES
  WITH (SIZE = '3xsmall');
```

To resize the source after creation:

```sql
ALTER SOURCE auction_load SET (SIZE = 'large');
```

The smallest source size (`3xsmall`) is a resonable default to get started. For more details on sizing sources, check the [`CREATE SOURCE`](../#sizing-a-source) documentation page.

## Related pages

- [`CREATE SOURCE`](../)

[`bigint`]: /sql/types/bigint
[`text`]: /sql/types/text
[`timestamp with time zone`]: /sql/types/timestamp
[feature request]: https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=A-integration&template=02-feature.yml
