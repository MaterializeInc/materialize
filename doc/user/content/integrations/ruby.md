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

To connect to Materialize using `pg`:

```ruby
require 'pg'

conn = PG.connect(host:"MATERIALIZE_HOST", port: 6875, user: "MATERIALIZE_USERNAME", password: "MATERIALIZE_PASSWORD")
```

If you don't have a `pg` gem, you can install it with:

```bash
gem install pg
```

## Create tables

Most data in Materialize will stream in via an external system, but a [table](/sql/create-table/) can be helpful for supplementary data. For example, you can use a table to join slower-moving reference or lookup data with a stream.

To create a table named `countries` in Materialize:

```ruby
require 'pg'

conn = PG.connect(host:"MATERIALIZE_HOST", port: 6875, user: "MATERIALIZE_USERNAME", password: "MATERIALIZE_PASSWORD")

conn.exec("CREATE TABLE IF NOT EXISTS countries (code CHAR(2), name TEXT);")

res  = conn.exec('SHOW TABLES')

res.each do |row|
  puts row
end
```

## Insert data into tables

**Basic Example:** [Insert a row](https://materialize.com/docs/sql/insert/) of data into a table named `countries` in Materialize:

```ruby
require 'pg'

conn = PG.connect(host:"MATERIALIZE_HOST", port: 6875, user: "MATERIALIZE_USERNAME", password: "MATERIALIZE_PASSWORD")

conn.exec("INSERT INTO countries (code, name) VALUES ('US', 'United States');")
conn.exec("INSERT INTO countries (code, name) VALUES ('CA', 'Canada');")

res  = conn.exec('SELECT * FROM countries')

res.each do |row|
  puts row
end
```

## Query

Querying Materialize is identical to querying a PostgreSQL database: Ruby executes the query, and Materialize returns the state of the view, source, or table at that point in time.

Because Materialize keeps results incrementally updated, response times are much faster than traditional database queries, and polling (repeatedly querying) a view doesn't impact performance.

To query the `countries` table using a `SELECT` statement:

```ruby
require 'pg'

conn = PG.connect(host:"MATERIALIZE_HOST", port: 6875, user: "MATERIALIZE_USERNAME", password: "MATERIALIZE_PASSWORD")

res  = conn.exec('SELECT * FROM countries')

res.each do |row|
  puts row
end
```

For more details, see the [`exec` instance method](https://rubydoc.info/gems/pg/0.10.0/PGconn#exec-instance_method) documentation.

## Manage sources, views, and indexes

Typically, you create sources, views, and indexes when deploying Materialize, but it's also possible to use a Ruby app to execute common DDL statements.

### Create a source from Ruby

```ruby
require 'pg'

conn = PG.connect(host:"MATERIALIZE_HOST", port: 6875, user: "MATERIALIZE_USERNAME", password: "MATERIALIZE_PASSWORD")

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

conn = PG.connect(host:"MATERIALIZE_HOST", port: 6875, user: "MATERIALIZE_USERNAME", password: "MATERIALIZE_PASSWORD")

# Create a view
view = conn.exec(
    "CREATE MATERIALIZED VIEW IF NOT EXISTS counter_sum AS
      SELECT sum(counter)
    FROM counter;"
);
puts view.inspect

# Show the view
res = conn.exec("SHOW MATERIALIZED VIEWS")
res.each do |row|
  puts row
end
```

For more information, see [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/).


## Stream

To take full advantage of incrementally updated materialized views from a Ruby application, instead of [querying](#query) Materialize for the state of a view at a point in time, use a [`SUBSCRIBE` statement](/sql/subscribe/) to request a stream of updates as the view changes.

To read a stream of updates from an existing materialized view, open a long-lived transaction with `BEGIN` and use [`SUBSCRIBE` with `FETCH`](/sql/subscribe/#subscribing-with-fetch) to repeatedly fetch all changes to the view since the last query:

```ruby
require 'pg'

# Locally running instance:
conn = PG.connect(host:"MATERIALIZE_HOST", port: 6875, user: "MATERIALIZE_USERNAME", password: "MATERIALIZE_PASSWORD")
conn.exec('BEGIN')
conn.exec('DECLARE c CURSOR FOR SUBSCRIBE counter_sum')

while true
  conn.exec('FETCH c') do |result|
    result.each do |row|
      puts row
    end
  end
end
```

Each `result` of the [SUBSCRIBE output format](/sql/subscribe/#output) has exactly object. When a row of a subscribed view is **updated,** two objects will show up:

```json
...
{"mz_timestamp"=>"1648126887708", "mz_diff"=>"1", "sum"=>"1"}
{"mz_timestamp"=>"1648126887708", "mz_diff"=>"1", "sum"=>"2"}
{"mz_timestamp"=>"1648126887708", "mz_diff"=>"1", "sum"=>"3"}
{"mz_timestamp"=>"1648126897364", "mz_diff"=>"-1", "sum"=>"1"}
...
```

An `mz_diff` value of `-1` indicates Materialize is deleting one row with the included values.  An update is just a retraction (`mz_diff: '-1'`) and an insertion (`mz_diff: '1'`) with the same `mz_timestamp`.

## Clean up

To clean up the sources, views, and tables that we created, first connect to Materialize using a [PostgreSQL client](/integrations/sql-clients/) and then, run the following commands:

```mzsql
DROP MATERIALIZED VIEW IF EXISTS counter_sum;
DROP SOURCE IF EXISTS counter;
DROP TABLE IF EXISTS countries;
```

## Ruby ORMs

ORM frameworks like **Active Record** tend to run complex introspection queries that may use configuration settings, system tables or features not yet implemented in Materialize. This means that even if a tool is compatible with PostgreSQL, it’s not guaranteed that the same integration will work out-of-the-box.

The level of support for these tools will improve as we extend the coverage of `pg_catalog` in Materialize and join efforts with each community to make the integrations Just Work™️.
