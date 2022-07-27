---
title: "Ruby cheatsheet"
description: "Use Ruby to connect, insert, manage, query and stream from Materialize."
aliases:
  - /guides/ruby/
menu:
  main:
    parent: 'client-libraries'
    name: 'Ruby'
---

Materialize is **wire-compatible** with PostgreSQL, which means that Ruby applications can use common PostgreSQL clients to interact with Materialize. In this guide, we'll use the  [`pg` gem](https://rubygems.org/gems/pg/) to connect to Materialize and issue SQL commands.

## Connect

To connect to a local instance of Materialize using `pg`:

```ruby
require 'pg'

conn = PG.connect(host:"127.0.0.1", port: 6875, user: "materialize")
```

If you don't have a `pg` gem, you can install it with:

```bash
gem install pg
```

## Stream

To take full advantage of incrementally updated materialized views from a Ruby application, instead of [querying](#query) Materialize for the state of a view at a point in time, use a [`TAIL` statement](/sql/tail/) to request a stream of updates as the view changes.

To read a stream of updates from an existing materialized view, open a long-lived transaction with `BEGIN` and use [`TAIL` with `FETCH`](/sql/tail/#tailing-with-fetch) to repeatedly fetch all changes to the view since the last query:

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

Each `result` of the [TAIL output format](/sql/tail/#output) has exactly object. When a row of a tailed view is **updated,** two objects will show up:

```json
...
{"mz_timestamp"=>"1648126887708", "mz_diff"=>"1", "my_column"=>"1"}
{"mz_timestamp"=>"1648126887708", "mz_diff"=>"1", "my_column"=>"2"}
{"mz_timestamp"=>"1648126887708", "mz_diff"=>"1", "my_column"=>"3"}
{"mz_timestamp"=>"1648126897364", "mz_diff"=>"-1", "my_column"=>"1"}
...
```

An `mz_diff` value of `-1` indicates Materialize is deleting one row with the included values.  An update is just a retraction (`mz_diff: '-1'`) and an insertion (`mz_diff: '1'`) with the same `mz_timestamp`.


## Query

Querying Materialize is identical to querying a PostgreSQL database: Ruby executes the query, and Materialize returns the state of the view, source, or table at that point in time.

Because Materialize maintains materialized views in memory, response times are much faster than traditional database queries, and polling (repeatedly querying) a view doesn't impact performance.

To query a view `my_view` using a `SELECT` statement:

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

Most data in Materialize will stream in via an external system, but a [table](/sql/create-table/) can be helpful for supplementary data. For example, you can use a table to join slower-moving reference or lookup data with a stream.

**Basic Example:** [Insert a row](https://materialize.com/docs/sql/insert/) of data into a table named `countries` in Materialize:

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

Typically, you create sources, views, and indexes when deploying Materialize, but it's also possible to use a Ruby app to execute common DDL statements.

### Create a source from Ruby

```ruby
require 'pg'

conn = PG.connect(host:"127.0.0.1", port: 6875, user: "materialize")

# Create a source
src = conn.exec(
    "CREATE SOURCE IF NOT EXISTS counter FROM LOAD GENERATOR counter;"
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

ORM frameworks like **Active Record** tend to run complex introspection queries that may use configuration settings, system tables or features not yet implemented in Materialize. This means that even if a tool is compatible with PostgreSQL, it’s not guaranteed that the same integration will work out-of-the-box.

The level of support for these tools will improve as we extend the coverage of `pg_catalog` in Materialize {{% gh 2157 %}} and join efforts with each community to make the integrations Just Work™️.
