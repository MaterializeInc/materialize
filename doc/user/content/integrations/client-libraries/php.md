---
title: "PHP cheatsheet"
description: "Use PHP PDO to connect, insert, manage, query and stream from Materialize."
aliases:
  - /guides/php-pdo/
  - /integrations/php/
menu:
  main:
    parent: 'client-libraries'
    name: "PHP"
---

Materialize is **wire-compatible** with PostgreSQL, which means that PHP applications can use common PostgreSQL clients to interact with Materialize. In this guide, we'll use the [PDO_PGSQL driver](https://www.php.net/manual/en/ref.pdo-pgsql.php) to connect to Materialize and issue SQL commands.

## Connect

To [connect](https://www.php.net/manual/en/ref.pdo-pgsql.connection.php) to Materialize using `PDO_PGSQL`:

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

$connection = connect('MATERIALIZE_HOST', 6875, 'materialize', 'MATERIALIZE_USERNAME', 'MATERIALIZE_PASSWORD');
```

You can add the above code to a `config.php` file and then include it in your application with `require 'connect.php';`.

## Create tables

Most data in Materialize will stream in via an external system, but a [`TABLE` in Materialize](/sql/create-table/) can be helpful for supplementary data. For example, you can use a table to join slower-moving reference or lookup data with a stream.

To create a table named `countries` in Materialize:

```php
<?php
// Include the Postgres connection details
require 'connect.php';

$sql = 'CREATE TABLE IF NOT EXISTS countries (
    code CHAR(2),
    name TEXT
)';

$statement = $connection->prepare($sql);
$statement->execute();
```

## Insert data into tables

**Basic Example:** [Insert a row](/sql/insert/) of data into the `countries` table in Materialize.

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

## Query

Querying Materialize is identical to querying a PostgreSQL database: PHP executes the query, and Materialize returns the state of the view, source, or table at that point in time.

Because Materialize keeps results incrementally updated, response times are much faster than traditional database queries, and polling (repeatedly querying) a view doesn't impact performance.

To query the `countries` table using a `SELECT` statement:

```php
<?php
// Include the Postgres connection details
require 'connect.php';

$sql = 'SELECT * FROM countries';
$statement = $connection->query($sql);

while (($row = $statement->fetch(PDO::FETCH_ASSOC)) !== false) {
    var_dump($row);
}
```

For more details, see the [PHP `PDOStatement`](https://www.php.net/manual/en/pdostatement.fetch.php) documentation.

## Manage sources, views, and indexes

Typically, you create sources, views, and indexes when deploying Materialize, although it is possible to use a PHP app to execute common DDL statements.

### Create a source from PHP

```php
<?php
// Include the Postgres connection details
require 'connect.php';

$sql = "CREATE SOURCE auction FROM LOAD GENERATOR AUCTION FOR ALL TABLES;";

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

$sql = "CREATE MATERIALIZED VIEW IF NOT EXISTS amount_sum AS SELECT SUM(amount) FROM bids;";

$statement = $connection->prepare($sql);
$statement->execute();

$views = "SHOW MATERIALIZED VIEWS";
$statement = $connection->query($views);
$result = $statement->fetchAll(PDO::FETCH_ASSOC);
var_dump($result);
```

For more information, see [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/).

## Stream

To take full advantage of incrementally updated materialized views from a PHP application, instead of [querying](#query) Materialize for the state of a view at a point in time, use a [`SUBSCRIBE` statement](/sql/subscribe/) to request a stream of updates as the view changes.

To read a stream of updates from an existing materialized view, open a long-lived transaction with `BEGIN` and use [`SUBSCRIBE` with `FETCH`](/sql/subscribe/#subscribing-with-fetch) to repeatedly fetch all changes to the view since the last query:

```php
<?php
// Include the Postgres connection details
require 'connect.php';

// Begin a transaction
$connection->beginTransaction();
// Declare a cursor
$statement = $connection->prepare('DECLARE c CURSOR FOR SUBSCRIBE amount_sum WITH (FETCH = true);');
// Execute the statement
$statement->execute();

/* Fetch all of the remaining rows in the result set */
while (true) {
    //$result = $statement->fetchAll();
    $subscribe = $connection->prepare('FETCH ALL c');
    $subscribe->execute();
    $result = $subscribe->fetchAll(PDO::FETCH_ASSOC);
    print_r($result);
}
```

The [SUBSCRIBE output format](/sql/subscribe/#output) of `result` is an array of view updates objects. When a row of a subscribed view is **updated,** two objects will show up in the `result` array:

```php
    ...
        Array
        (
            [0] => Array
                (
                    [mz_timestamp] => 1646310999683
                    [mz_diff] => 1
                    [sum] => 'value_1'
                )

        )
        Array
        (
            [0] => Array
                (
                    [mz_timestamp] => 1646311002682
                    [mz_diff] => -1
                    [sum] => 'value_2'
                )

        )
    ...
```

An `mz_diff` value of `-1` indicates Materialize is deleting one row with the included values.  An update is just a retraction (`mz_diff: '-1'`) and an insertion (`mz_diff: '1'`) with the same timestamp.

## Clean up

To clean up the sources, views, and tables that we created, first connect to Materialize using a [PostgreSQL client](/integrations/sql-clients/) and then, run the following commands:

```mzsql
DROP MATERIALIZED VIEW IF EXISTS ammount_sum;
DROP SOURCE IF EXISTS auction CASCADE;
DROP TABLE IF EXISTS countries;
```

## PHP ORMs

ORM frameworks like **Eloquent** tend to run complex introspection queries that may use configuration settings, system tables or features not yet implemented in Materialize. This means that even if a tool is compatible with PostgreSQL, it’s not guaranteed that the same integration will work out-of-the-box.

The level of support for these tools will improve as we extend the coverage of `pg_catalog` in Materialize and join efforts with each community to make the integrations Just Work™️.
