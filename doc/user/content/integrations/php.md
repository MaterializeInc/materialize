---
title: "PHP cheatsheet"
description: "Use PHP PDO to connect, insert, manage, query and stream from Materialize."
aliases:
  - /guides/php-pdo/
menu:
  main:
    parent: 'client-libraries'
    name: "PHP"
---

Materialize is **wire-compatible** with PostgreSQL, which means that PHP applications can use common PostgreSQL clients to interact with Materialize. In this guide, we'll use the [PDO_PGSQL driver](https://www.php.net/manual/en/ref.pdo-pgsql.php) to connect to Materialize and issue SQL commands.

## Connect

To [connect](https://www.php.net/manual/en/ref.pdo-pgsql.connection.php) to a local Materialize instance using `PDO_PGSQL`:

```php
<?php

function connect(string $host, int $port, string $db, string $user, string $password): PDO
{
    try {
        $dsn = "pgsql:host=$host;port=$port;dbname=$db;";

        // make a database connection
        return new PDO(
            $dsn,
            $user,
            $password,
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
        );
    } catch (PDOException $e) {
        die($e->getMessage());
    }
}

$connection = connect('localhost', 6875, 'materialize', 'materialize', 'materialize');
```

You can add the above code to a `config.php` file and then include it in your application with `require 'connect.php';`.

## Stream

To take full advantage of incrementally updated materialized views from a PHP application, instead of [querying](#query) Materialize for the state of a view at a point in time, use a [`TAIL` statement](/sql/tail/) to request a stream of updates as the view changes.

To read a stream of updates from an existing materialized view, open a long-lived transaction with `BEGIN` and use [`TAIL` with `FETCH`](/sql/tail/#tailing-with-fetch) to repeatedly fetch all changes to the view since the last query:

```php
<?php
// Include the Postgres connection details
require 'connect.php';

// Begin a transaction
$connection->beginTransaction();
// Declare a cursor
$statement = $connection->prepare('DECLARE c CURSOR FOR TAIL demo');
// Execute the statement
$statement->execute();

/* Fetch all of the remaining rows in the result set */
while (true) {
    //$result = $statement->fetchAll();
    $tail = $connection->prepare('FETCH ALL c');
    $tail->execute();
    $result = $tail->fetchAll(PDO::FETCH_ASSOC);
    print_r($result);
}
```

The [TAIL output format](/sql/tail/#output) of `result` is an array of view updates objects. When a row of a tailed view is **updated,** two objects will show up in the `result` array:

```php
    ...
        Array
        (
            [0] => Array
                (
                    [mz_timestamp] => 1646310999683
                    [mz_diff] => 1
                    [my_column_one] => 'value'
                )

        )
        Array
        (
            [0] => Array
                (
                    [mz_timestamp] => 1646311002682
                    [mz_diff] => -1
                    [my_column_one] => 'value'
                )

        )
    ...
```

An `mz_diff` value of `-1` indicates Materialize is deleting one row with the included values.  An update is just a retraction (`mz_diff: '-1'`) and an insertion (`mz_diff: '1'`) with the same timestamp.

## Query

Querying Materialize is identical to querying a PostgreSQL database: PHP executes the query, and Materialize returns the state of the view, source, or table at that point in time.

Because Materialize maintains materialized views in memory, response times are much faster than traditional database queries, and polling (repeatedly querying) a view doesn't impact performance.

To query a view `my_view` using a `SELECT` statement:

```php
<?php
// Include the Postgres connection details
require 'connect.php';

$sql = 'SELECT * FROM my_view';
$statement = $connection->query($sql);

while (($row = $statement->fetch(PDO::FETCH_ASSOC)) !== false) {
    var_dump($row);
}
```

For more details, see the [PHP `PDOStatement`](https://www.php.net/manual/en/pdostatement.fetch.php) documentation.

## Insert data into tables

Most data in Materialize will stream in via an external system, but a [`TABLE` in Materialize](/sql/create-table/) can be helpful for supplementary data. For example, you can use a table to join slower-moving reference or lookup data with a stream.

**Basic Example:** [Insert a row](/sql/insert/) of data into a table named `countries` in Materialize.

```php
<?php
// Include the Postgres connection details
require 'connect.php';

$sql = 'INSERT INTO countries (name, code) VALUES (?, ?)';
$statement = $connection->prepare($sql);
$statement->execute(['United States', 'US']);
$statement->execute(['Canada', 'CA']);
$statement->execute(['Mexico', 'MX']);
$statement->execute(['Germany', 'DE']);

$countStmt = "SELECT COUNT(*) FROM countries";
$count = $connection->query($countStmt);
while (($row = $count->fetch(PDO::FETCH_ASSOC)) !== false) {
    var_dump($row);
}
```

## Manage sources, views, and indexes

Typically, you create sources, views, and indexes when deploying Materialize, although it is possible to use a PHP app to execute common DDL statements.

### Create a source from PHP

```php
<?php
// Include the Postgres connection details
require 'connect.php';

$sql = "CREATE SOURCE counter FROM LOAD GENERATOR COUNTER";

$statement = $connection->prepare($sql);
$statement->execute();

$sources = "SHOW SOURCES";
$statement = $connection->query($sources);
$result = $statement->fetchAll(PDO::FETCH_ASSOC);
var_dump($result);

```

For more information, see [`CREATE SOURCE`](/sql/create-source/).

### Create a view from PHP

```php
<?php
// Include the Postgres connection details
require 'connect.php';

$sql = "CREATE VIEW market_orders_2 AS
            SELECT
                val->>'symbol' AS symbol,
                (val->'bid_price')::float AS bid_price
            FROM (SELECT text::jsonb AS val FROM market_orders_raw_2)";

$statement = $connection->prepare($sql);
$statement->execute();

$views = "SHOW VIEWS";
$statement = $connection->query($views);
$result = $statement->fetchAll(PDO::FETCH_ASSOC);
var_dump($result);
```

For more information, see [`CREATE VIEW`](/sql/create-view/).

## PHP ORMs

ORM frameworks like **Eloquent** tend to run complex introspection queries that may use configuration settings, system tables or features not yet implemented in Materialize. This means that even if a tool is compatible with PostgreSQL, it’s not guaranteed that the same integration will work out-of-the-box.

The level of support for these tools will improve as we extend the coverage of `pg_catalog` in Materialize {{% gh 2157 %}} and join efforts with each community to make the integrations Just Work™️.
