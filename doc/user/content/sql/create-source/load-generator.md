---
title: "CREATE SOURCE: Load generator"
description: "Using Materialize's built-in load generators"
pagerank: 10
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
**COUNTER** | Use the [counter](#counter) load generator.
**AUCTION** | Use the [auction](#auction) load generator.
**IF NOT EXISTS**  | Do nothing (except issuing a notice) if a source with the same name already exists.
**TICK INTERVAL**  | The interval at which the next datum should be emitted. Defaults to one second.
**FOR ALL TABLES** | Creates subsources for all data sources in the load generator.
**FOR TABLES** _table_name_ | Creates subsources for specific data sources in the load generator.

### `WITH` options

Field                                | Value     | Description
-------------------------------------|-----------|-------------------------------------
`SIZE`                               | `text`    | Default: `3xsmall`. The [size](../#sizing-a-source) for the source. Accepts values: `3xsmall`, `2xsmall`, `xsmall`, `small`, `medium`, `large`.

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

    Field | Type       | Describes
    ------|------------|----------
    id    | [`bigint`] | A unique identifier for the organization.
    name  | [`text`]   | The organization's name.

  * `users` describes the users that belong to each organization.

    Field     | Type       | Describes
    ----------|------------|----------
    `id`      | [`bigint`] | A unique identifier for the user.
    `org_id`  | [`bigint`] | The identifier of the organization to which the user belongs. References `organizations.id`.
    `name`    | [`text`]   | The user's name.

  * `accounts` describes the account associated with each organization.

    Field     | Type       | Describes
    ----------|------------|----------
    `id`      | [`bigint`] | A unique identifier for the account.
    `org_id`  | [`bigint`] | The identifier of the organization to which the account belongs. References `organizations.id`.
    `balance` | [`bigint`] | The balance of the account in dollars.

  * `auctions` describes all past and ongoing auctions.

    Field      | Type                         | Describes
    -----------|------------------------------|----------
    `id`       | [`bigint`]                   | A unique identifier for the auction.
    `seller`   | [`bigint`]                   | The identifier of the user selling the item. References `users.id`.
    `item`     | [`text`]                     | The name of the item being sold.
    `end_time` | [`timestamp with time zone`] | The time at which the auction closes.

  * `bids` describes the bids placed in each auction.

    Field        | Type                         | Describes
    -------------|------------------------------|----------
    `id`         | [`bigint`]                   | A unique identifier for the bid.
    `buyer`      | [`bigint`]                   | The identifier vof the user placing the bid. References `users.id`.
    `auction_id` | [`text`]                     | The identifier of the auction in which the bid is placed. References `auctions.id`.
    `amount`     | [`bigint`]                   | The bid amount in dollars.
    `bid_time`   | [`timestamp with time zone`] | The time at which the bid was placed.

The organizations, users, and accounts are fixed at the time the source
is created. Each tick interval, either a new auction is started, or a new bid
is placed in the currently ongoing auction.

## Examples

### Creating a counter load generator

To create a load generator source that emits the next number in the sequence every
second:

```sql
CREATE SOURCE counter
FROM LOAD GENERATOR COUNTER
WITH (SIZE '3xsmall');
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
(3 rows)
```

### Creating an auction load generator

To create the load generator source and its associated views:

```sql
CREATE SOURCE auction_house
    FROM LOAD GENERATOR AUCTION
    FOR ALL TABLES
    WITH (SIZE '3xsmall');
```

To display the created subsources:

```sql
SHOW SOURCES;
```
```nofmt
     name      |      type      |  size
---------------+----------------+---------
 accounts      | subsource      | 3xsmall
 auction_house | load-generator | 3xsmall
 auctions      | subsource      | 3xsmall
 bids          | subsource      | 3xsmall
 organizations | subsource      | 3xsmall
 users         | subsource      | 3xsmall
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
(3 rows)
```

### Sizing a source

To provision a specific amount of CPU and memory to a source on creation, use the `SIZE` option:

```sql
CREATE SOURCE auction_load FROM LOAD GENERATOR AUCTION
WITH (SIZE = 'xsmall');
```

To resize the source after creation:

```sql
ALTER SOURCE auction_load SET (SIZE = 'large');
```

The smallest source size (`3xsmall`) is a resonable default to get started. For more details on sizing sources, check the [`CREATE SOURCE`](../) documentation page.

## Related pages

- [`CREATE SOURCE`](../)

[`bigint`]: /sql/types/bigint
[`text`]: /sql/types/text
[`timestamp with time zone`]: /sql/types/timestamp
[feature request]: https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=A-integration&template=02-feature.yml
