---
title: "Rust cheatsheet"
description: "Use Rust postgres-openssl to connect, insert, manage, query and stream from Materialize."
aliases:
  - /guides/rust
  - /integrations/rust/
menu:
  main:
    parent: 'client-libraries'
    name: "Rust"
---

Materialize is **wire-compatible** with PostgreSQL, which means that Rust applications can use common PostgreSQL clients to interact with Materialize. In this guide, we'll use the [`postgres-openssl`](https://docs.rs/postgres-openssl/latest/postgres_openssl/) crate (the TLS support for [`tokio-postgres`](https://crates.io/crates/tokio-postgres) via `openssl`) to connect to Materialize and issue SQL commands.

## Connect

To connect to Materialize using `postgres-openssl`:

```rust
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres::{Client, Error};
use postgres_openssl::MakeTlsConnector;

pub(crate) fn create_client() -> Result<Client, Error> {
    let mut builder = SslConnector::builder(SslMethod::tls()).expect("Error creating builder.");
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());

    let config = "postgres://MATERIALIZE_USERNAME:APP_SPECIFIC_PASSWORD@MATERIALIZE_HOST:6875/materialize?sslmode=require";
    Client::connect(config, connector)
}
```

## Create tables

Most data in Materialize will stream in via an external system, but a [table](/sql/create-table/) can be helpful for supplementary data. For example, you can use a table to join slower-moving reference or lookup data with a stream.

To create a table named `countries` in Materialize:

```rust
use postgres::Error;

use crate::connection::create_client;

pub(crate) fn create_table() -> Result<u64, Error> {
    let mut client = create_client().expect("Error creating client.");

    client.execute(
        "
        CREATE TABLE IF NOT EXISTS countries (
            code CHAR(2),
            name TEXT
        );
    ",
        &[],
    )
}
```

## Insert data into tables

To [insert a row](/sql/insert/) of data into a table named `countries` in Materialize:

```rust
use postgres::Error;

use crate::connection::create_client;

pub(crate) fn insert() -> Result<u64, Error> {
    let mut client = create_client().expect("Error creating client.");

    let code = "GH";
    let name = "Ghana";

    client.execute(
        "INSERT INTO countries(code, name) VALUES($1, $2)",
        &[&code, &name],
    )
}
```

## Query

Querying Materialize is identical to querying a PostgreSQL database. Here's how to do a SELECT statement:

```rust
use crate::connection::create_client;

pub(crate) fn run_query () {
    let mut client = create_client().expect("Error creating client.");

    let results = client.query("SELECT code, name FROM countries;", &[]).expect("Error running query.");

    for row in results {
        println!("{:} - {:}", row.get::<usize, String>(0), row.get::<usize, String>(1));
    };
}
```

## Manage sources, views, and indexes

Typically, you create sources, views, and indexes when deploying Materialize, although it is possible to use a Rust app to execute common DDL statements.

### Create a source from Rust

```rust
use postgres::Error;

use crate::connection::create_client;

pub(crate) fn create_source() -> Result<u64, Error> {
    let mut client = create_client().expect("Error creating client.");

    client.execute(
        "
        CREATE SOURCE IF NOT EXISTS auction
        FROM LOAD GENERATOR AUCTION FOR ALL TABLES;
    ",
        &[],
    )
}
```

### Create a view from Rust

```rust
use postgres::Error;

use crate::connection::create_client;

pub(crate) fn create_materialized_view() -> Result<u64, Error> {
    let mut client = create_client().expect("Error creating client.");

    client.execute(
        "
        CREATE MATERIALIZED VIEW IF NOT EXISTS amount_sum AS
        SELECT sum(amount)
        FROM bids;
    ",
        &[],
    )
}
```

## Stream

Materialize is designed to stream changes to views. To subscribe to a stream of updates in Rust, you can use the `SUBSCRIBE` feature. Here's how to subscribe to a stream:

```rust
use crate::connection::create_client;

pub(crate) fn subscribe() {
    let mut client = create_client().expect("Error creating client.");
    let mut transaction = client.transaction().expect("Error creating transaction.");
    transaction.execute("DECLARE c CURSOR FOR SUBSCRIBE (SELECT sum::text FROM amount_sum) WITH (SNAPSHOT = false);", &[]).expect("Error creating cursor.");

    loop {
        let results = transaction.query("FETCH ALL c;", &[]).expect("Error running fetch.");
        for row in results {
            println!("{:}", row.get::<usize, String>(2));
        }
    }
}
```

The [SUBSCRIBE output format](/sql/subscribe/#output) of the `amount_sum` view contains all of the columns of the view, prepended with several additional columns that describe the nature of the update.

## Clean up

To clean up the sources, views, and tables that we created, first connect to Materialize using a [PostgreSQL client](/integrations/sql-clients/) and then, run the following commands:

```mzsql
DROP MATERIALIZED VIEW IF EXISTS amount_sum;
DROP SOURCE IF EXISTS auction CASCADE;
DROP TABLE IF EXISTS countries;
```

## ORM

Rust ORMs like `Diesel` and `sqlx` tend to run complex introspection queries that may use configuration settings, system tables or features not yet implemented in Materialize. This means that even if a tool is compatible with PostgreSQL, it’s not guaranteed that the same integration will work out-of-the-box.

The level of support for these tools will improve as we extend the coverage of `pg_catalog` in Materialize and join efforts with each community to make the integrations Just Work™️.
