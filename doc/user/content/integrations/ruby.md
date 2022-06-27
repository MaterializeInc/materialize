---
title: "Ruby Cheatsheet"
description: "Use Ruby to connect, insert, manage, query and stream from Materialize."
aliases:
  - /guides/ruby/
menu:
  main:
    parent: 'client-libraries'
    name: 'Ruby'
---

Materialize is **PostgreSQL-compatible**, which means that Ruby applications can use any existing PostgreSQL client to interact with Materialize as if it were a PostgreSQL database. In this guide, we'll use the  [`pg` gem](https://rubygems.org/gems/pg/) to connect to Materialize and issue PostgreSQL commands.

## Connect

You connect to Materialize the same way you [connect to PostgreSQL with `pg`](https://github.com/ged/ruby-pg). If you don't have a `pg` gem, you can install it with:

```bash
gem install pg
```

### Local Instance

You can connect to a local Materialize instance just as you would connect to a PostgreSQL instance:

```ruby
require 'pg'

conn = PG.connect(host:"127.0.0.1", port: 6875, user: "materialize")
```

## Stream

To take full advantage of incrementally updated materialized views from a Ruby application, instead of [querying](#query) Materialize for the state of a view at a point in time, use [a `TAIL` statement](/sql/tail/) to request a stream of updates as the view changes.

To read a stream of updates from an existing materialized view, open a long-lived transaction with `BEGIN` and use [`TAIL` with `FETCH`](/sql/tail/#tailing-with-fetch) to repeatedly fetch all changes to the view since the last query.

```ruby
require 'pg'

# Locally running instance:
conn = PG.connect(host:"127.0.0.1", port: 6875, user: "materialize")
conn.exec('BEGIN')
conn.exec('DECLARE c CURSOR FOR TAIL my_view')

while true
  conn.exec('FETCH c') do |result|
    result.each do |row|
      puts row
    end
  end
end
```

The [TAIL Output format](/sql/tail/#output) of `res.rows` is an array of view update objects. When a row of a tailed view is **updated,** two objects will show up in the `rows` array:

```json
...
{"mz_timestamp"=>"1648126887708", "mz_diff"=>"1", "my_column"=>"1"}
{"mz_timestamp"=>"1648126887708", "mz_diff"=>"1", "my_column"=>"2"}
{"mz_timestamp"=>"1648126887708", "mz_diff"=>"1", "my_column"=>"3"}
{"mz_timestamp"=>"1648126897364", "mz_diff"=>"-1", "my_column"=>"1"}
...
```

An `mz_diff` value of `-1` indicates Materialize is deleting one row with the included values.  An update is just a deletion (`mz_diff: '-1'`) and an insertion (`mz_diff: '1'`) with the same `mz_timestamp`.


## Query

Querying Materialize is identical to querying a traditional PostgreSQL database: Ruby executes the query, and Materialize returns the state of the view, source, or table at that point in time.

Because Materialize maintains materialized views in memory, response times are much faster than traditional database queries, and polling (repeatedly querying) a view doesn't impact performance.

Query a view `my_view` with a select statement:

```ruby
require 'pg'

conn = PG.connect(host:"127.0.0.1", port: 6875, user: "materialize")

res  = conn.exec('SELECT * FROM my_view')

res.each do |row|
  puts row
end
```

For more details, see the  [`exec` instance method](https://rubydoc.info/gems/pg/0.10.0/PGconn#exec-instance_method) documentation.

## Insert data into tables

Most data in Materialize will stream in via a `SOURCE`, but a [`TABLE` in Materialize](https://materialize.com/docs/sql/create-table/) can be helpful for supplementary data. For example, use a table to join slower-moving reference or lookup data with a stream.

**Basic Example:** [Insert a row](https://materialize.com/docs/sql/insert/) of data into a table named `countries` in Materialize.

```ruby
require 'pg'

conn = PG.connect(host:"127.0.0.1", port: 6875, user: "materialize")

conn.exec("INSERT INTO my_table (my_column) VALUES ('some_value')")

res  = conn.exec('SELECT * FROM my_table')

res.each do |row|
  puts row
end
```

## Manage sources, views, and indexes

Typically, you create sources, views, and indexes when deploying Materialize, although it is possible to use a Ruby app to execute common DDL statements.

### Create a source from Ruby

```ruby
require 'pg'

conn = PG.connect(host:"127.0.0.1", port: 6875, user: "materialize")

# Create a source
src = conn.exec(
    "CREATE SOURCE IF NOT EXISTS market_orders_raw FROM PUBNUB
            SUBSCRIBE KEY 'sub-c-4377ab04-f100-11e3-bffd-02ee2ddab7fe'
            CHANNEL 'pubnub-market-orders'"
);

puts src.inspect

# Show the source
res = conn.exec("SHOW SOURCES")
res.each do |row|
  puts row
end
```

For more information, see [`CREATE SOURCE`](/sql/create-source/).

### Create a view from Ruby

```ruby
require 'pg'

conn = PG.connect(host:"127.0.0.1", port: 6875, user: "materialize")

# Create a view
view = conn.exec(
    "CREATE VIEW market_orders_2 AS
            SELECT
                val->>'symbol' AS symbol,
                (val->'bid_price')::float AS bid_price
            FROM (SELECT text::jsonb AS val FROM market_orders_raw)"
);
puts view.inspect

# Show the view
res = conn.exec("SHOW VIEWS")
res.each do |row|
  puts row
end
```

For more information, see [`CREATE VIEW`](/sql/create-view/).

## Ruby ORMs

Materialize doesn't currently support the full catalog of PostgreSQL system metadata API endpoints, including the system calls that object relational mapping systems (ORMs) like **Active Record** use to introspect databases and do extra work behind the scenes. This means that ORM system attempts to interact with Materialize will currently fail. Once [full `pg_catalog` support](https://github.com/MaterializeInc/materialize/issues/2157) is implemented, the features that depend on  `pg_catalog` may work properly.
