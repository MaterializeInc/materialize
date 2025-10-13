---
title: "Java cheatsheet"
description: "Use the PostgreSQL JDBC Driver to connect, insert, manage, query and stream from Materialize."
aliases:
  - /guides/java-jdbc/
  - /integrations/java-jdbc/
menu:
  main:
    parent: 'client-libraries'
    name: "Java"
---

Materialize is **wire-compatible** with PostgreSQL, which means that Java
applications can use common PostgreSQL clients to interact with Materialize.
This guide uses the [PostgreSQL JDBC Driver](https://jdbc.postgresql.org/) to
connect to Materialize and issue SQL commands.

## Connect

To [connect](https://jdbc.postgresql.org/documentation/head/connect.html) to Materialize using the PostgreSQL JDBC Driver:

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class App {

    private final String url = "jdbc:postgresql://MATERIALIZE_HOST:6875/materialize";
    private final String user = "MATERIALIZE_USERNAME";
    private final String password = "MATERIALIZE_PASSWORD";

    /**
     * Connect to Materialize
     *
     * @return a Connection object
     */
    public Connection connect() {
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        props.setProperty("ssl","true");
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, props);
            System.out.println("Connected to Materialize successfully!");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        return conn;
    }

    public static void main(String[] args) {
        App app = new App();
        app.connect();
    }
}
```

To establish the connection to Materialize, call the `getConnection()` method on the `DriverManager` class.

## Create tables

Most data in Materialize will stream in via an external system, but a [table](/sql/create-table/) can be helpful for supplementary data. For example, you can use a table to join slower-moving reference or lookup data with a stream.

To create a table named `countries` in Materialize:

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.PreparedStatement;

public class App {

    private final String url = "jdbc:postgresql://MATERIALIZE_HOST:6875/materialize";
    private final String user = "MATERIALIZE_USERNAME";
    private final String password = "MATERIALIZE_PASSWORD";

    /**
     * Connect to Materialize
     *
     * @return a Connection object
     */
    public Connection connect() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        props.setProperty("ssl","true");

        return DriverManager.getConnection(url, props);

    }

    public void createTable() {

        String SQL = "CREATE TABLE IF NOT EXISTS countries (code CHAR(2), name TEXT)";

        try (Connection conn = connect()) {
            Statement st = conn.createStatement();
            st.execute(SQL);
            System.out.println("Table created.");
            st.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public static void main(String[] args) {
        App app = new App();
        app.createTable();
    }

}
```

## Insert data into tables

**Basic Example:** [Insert a row](/sql/insert/) of data into a table named `countries` in Materialize:

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.PreparedStatement;

public class App {

    private final String url = "jdbc:postgresql://MATERIALIZE_HOST:6875/materialize";
    private final String user = "MATERIALIZE_USERNAME";
    private final String password = "MATERIALIZE_PASSWORD";

    /**
     * Connect to Materialize
     *
     * @return a Connection object
     */
    public Connection connect() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        props.setProperty("ssl","true");

        return DriverManager.getConnection(url, props);

    }

    public void insert() {

        try (Connection conn = connect()) {
            String code = "GH";
            String name = "Ghana";
            PreparedStatement st = conn.prepareStatement("INSERT INTO countries(code, name) VALUES(?, ?)");
            st.setString(1, code);
            st.setString(2, name);
            int rowsDeleted = st.executeUpdate();
            System.out.println(rowsDeleted + " rows inserted.");
            st.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public static void main(String[] args) {
        App app = new App();
        app.insert();
    }
}
```

## Query

Querying Materialize is identical to querying a PostgreSQL database: Java executes the query, and Materialize returns the state of the view, source, or table at that point in time.

Because Materialize keeps results incrementally updated, response times are much faster than traditional database queries, and polling (repeatedly querying) a view doesn't impact performance.

To query the `countries` using a `SELECT` statement:

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.sql.ResultSet;
import java.sql.Statement;

public class App {

    private final String url = "jdbc:postgresql://MATERIALIZE_HOST:6875/materialize";
    private final String user = "MATERIALIZE_USERNAME";
    private final String password = "MATERIALIZE_PASSWORD";

    /**
     * Connect to Materialize
     *
     * @return a Connection object
     */
    public Connection connect() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        props.setProperty("ssl","true");

        return DriverManager.getConnection(url, props);

    }

    public void query() {

        String SQL = "SELECT * FROM countries";

        try (Connection conn = connect();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(SQL)) {
            while (rs.next()) {
                System.out.println(rs.getString("code") + " " + rs.getString("name"));
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public static void main(String[] args) {
        App app = new App();
        app.query();
    }
}
```

For more details, see the [JDBC](https://jdbc.postgresql.org/documentation/head/query.html) documentation.

## Manage sources, views, and indexes

Typically, you create sources, views, and indexes when deploying Materialize, although it is possible to use a Java app to execute common DDL statements.

### Create a source from Java

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.PreparedStatement;

public class App {

    private final String url = "jdbc:postgresql://MATERIALIZE_HOST:6875/materialize";
    private final String user = "MATERIALIZE_USERNAME";
    private final String password = "MATERIALIZE_PASSWORD";

    /**
     * Connect to Materialize
     *
     * @return a Connection object
     */
    public Connection connect() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        props.setProperty("ssl","true");

        return DriverManager.getConnection(url, props);

    }

    public void source() {

        String SQL = "CREATE SOURCE auction FROM LOAD GENERATOR AUCTION FOR ALL TABLES;";

        try (Connection conn = connect()) {
            Statement st = conn.createStatement();
            st.execute(SQL);
            System.out.println("Source created.");
            st.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public static void main(String[] args) {
        App app = new App();
        app.source();
    }
}
```

For more information, see [`CREATE SOURCE`](/sql/create-source/).

### Create a view from Java

```java
    public void view() {

        String SQL = "CREATE MATERIALIZED VIEW IF NOT EXISTS amount_sum AS "
                   + "SELECT sum(amount)"
                   + "FROM bids;";

        try (Connection conn = connect()) {
            Statement st = conn.createStatement();
            st.execute(SQL);
            System.out.println("View created.");
            st.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }
```

For more information, see [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/).

## Stream

To take full advantage of incrementally updated materialized views from a Java application, instead of [querying](#query) Materialize for the state of a view at a point in time, use a [`SUBSCRIBE` statement](/sql/subscribe/) to request a stream of updates as the view changes.

To read a stream of updates from an existing materialized view, open a long-lived transaction with `BEGIN` and use [`SUBSCRIBE` with `FETCH`](/sql/subscribe/#subscribing-with-fetch) to repeatedly fetch all changes to the view since the last query:

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.sql.ResultSet;
import java.sql.Statement;

public class App {

    private final String url = "jdbc:postgresql://MATERIALIZE_HOST:6875/materialize";
    private final String user = "MATERIALIZE_USERNAME";
    private final String password = "MATERIALIZE_PASSWORD";

    /**
     * Connect to Materialize
     *
     * @return a Connection object
     */
    public Connection connect() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        props.setProperty("ssl","true");

        return DriverManager.getConnection(url, props);

    }

    public void subscribe() {
        try (Connection conn = connect()) {

            Statement stmt = conn.createStatement();
            stmt.execute("BEGIN");
            stmt.execute("DECLARE c CURSOR FOR SUBSCRIBE amount_sum");
            while (true) {
                ResultSet rs = stmt.executeQuery("FETCH ALL c");
                if(rs.next()) {
                    System.out.println(rs.getString(1) + " " + rs.getString(2));
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public static void main(String[] args) {
        App app = new App();
        app.subscribe();
    }
}
```

The [`SUBSCRIBE` output format](/sql/subscribe/#output) of `rs` is a `ResultSet` of view updates. When a row of a subscribed view is **updated,** two objects will show up in the `rows` array:

```java
    ...
    1648567756801 1 value_3
    1648567761801 1 value_4
    1648567785802 -1 value_4
    ...
```

A `mz_diff` value of `-1` indicates that Materialize is deleting one row with the included values.  An update is just a retraction (`mz_diff: '-1'`) and an insertion (`mz_diff: '1'`) with the same timestamp.

## Clean up

To clean up the sources, views, and tables that we created, first connect to Materialize using a [PostgreSQL client](/integrations/sql-clients/) and then, run the following commands:

```mzsql
DROP MATERIALIZED VIEW IF EXISTS amount_sum;
DROP SOURCE IF EXISTS auction CASCADE;
DROP TABLE IF EXISTS countries;
```

## Java ORMs

ORM frameworks like **Hibernate** tend to run complex introspection queries that may use configuration settings, system tables or features not yet implemented in Materialize. This means that even if a tool is compatible with PostgreSQL, it’s not guaranteed that the same integration will work out-of-the-box.

The level of support for these tools will improve as we extend the coverage of `pg_catalog` in Materialize and join efforts with each community to make the integrations Just Work™️.
