---
title: "Client libraries"
description: "Connect via client libraries/SQL drivers"
disable_list: true
menu:
  main:
    parent: integrations
    name: "Client libraries"
    identifier: client-libraries
    weight: 15
---

Applications can use various common language-specific PostgreSQL client
libraries to interact with Materialize and **create relations**, **execute
queries** and **stream out results**.

{{< note >}}
Client libraries tend to run complex introspection queries that may use configuration settings, system tables or features not yet implemented in Materialize. This means that even if PostgreSQL is supported, it's **not guaranteed** that the same integration will work out-of-the-box.
{{</ note >}}

| Language | Tested drivers                                                  | Notes                                                 |
| -------- | --------------------------------------------------------------- | ----------------------------------------------------- |
| Go       | [`pgx`](https://github.com/jackc/pgx)                           | See the [Go cheatsheet](/integrations/client-libraries/golang/).       |
| Java     | [PostgreSQL JDBC driver](https://jdbc.postgresql.org/)          | See the [Java cheatsheet](/integrations/client-libraries/java-jdbc/).  |
| Node.js  | [`node-postgres`](https://node-postgres.com/)                   | See the [Node.js cheatsheet](/integrations/client-libraries/node-js/). |
| PHP      | [`pdo_pgsql`](https://www.php.net/manual/en/ref.pgsql.php)      | See the [PHP cheatsheet](/integrations/client-libraries/php/).         |
| Python   | [`psycopg2`](https://pypi.org/project/psycopg2/)                | See the [Python cheatsheet](/integrations/client-libraries/python/).   |
| Ruby     | [`pg` gem](https://rubygems.org/gems/pg/)                       | See the [Ruby cheatsheet](/integrations/client-libraries/ruby/).       |
| Rust     | [`postgres-openssl`](https://crates.io/crates/postgres-openssl) | See the [Rust cheatsheet](/integrations/client-libraries/rust/).       |

👋 _Is there another client library you'd like to use with Materialize? Submit a
[feature
request](https://github.com/MaterializeInc/materialize/discussions/new?category=feature-requests&labels=A-integration)._
