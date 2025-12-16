<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)  /  [Tools and
integrations](/docs/self-managed/v25.2/integrations/)  /  [Client
libraries](/docs/self-managed/v25.2/integrations/client-libraries/)

</div>

# Java cheatsheet

Materialize is **wire-compatible** with PostgreSQL, which means that
Java applications can use common PostgreSQL clients to interact with
Materialize. This guide uses the [PostgreSQL JDBC
Driver](https://jdbc.postgresql.org/) to connect to Materialize and
issue SQL commands.

## Connect

To
[connect](https://jdbc.postgresql.org/documentation/head/connect.html)
to Materialize using the PostgreSQL JDBC Driver:

<div class="highlight">

``` chroma
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

</div>

To establish the connection to Materialize, call the `getConnection()`
method on the `DriverManager` class.

## Create tables

Most data in Materialize will stream in via an external system, but a
[table](/docs/self-managed/v25.2/sql/create-table/) can be helpful for
supplementary data. For example, you can use a table to join
slower-moving reference or lookup data with a stream.

To create a table named `countries` in Materialize:

<div class="highlight">

``` chroma
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

</div>

## Insert data into tables

**Basic Example:** [Insert a row](/docs/self-managed/v25.2/sql/insert/)
of data into a table named `countries` in Materialize:

<div class="highlight">

``` chroma
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

</div>

## Query

Querying Materialize is identical to querying a PostgreSQL database:
Java executes the query, and Materialize returns the state of the view,
source, or table at that point in time.

Because Materialize keeps results incrementally updated, response times
are much faster than traditional database queries, and polling
(repeatedly querying) a view doesn’t impact performance.

To query the `countries` using a `SELECT` statement:

<div class="highlight">

``` chroma
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

</div>

For more details, see the
[JDBC](https://jdbc.postgresql.org/documentation/head/query.html)
documentation.

## Manage sources, views, and indexes

Typically, you create sources, views, and indexes when deploying
Materialize, although it is possible to use a Java app to execute common
DDL statements.

### Create a source from Java

<div class="highlight">

``` chroma
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

        String SQL = "CREATE SOURCE counter FROM LOAD GENERATOR COUNTER;";

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

</div>

For more information, see
[`CREATE SOURCE`](/docs/self-managed/v25.2/sql/create-source/).

### Create a view from Java

<div class="highlight">

``` chroma
    public void view() {

        String SQL = "CREATE MATERIALIZED VIEW IF NOT EXISTS counter_sum AS "
                   + "SELECT sum(counter)"
                   + "FROM counter;";

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

</div>

For more information, see
[`CREATE MATERIALIZED VIEW`](/docs/self-managed/v25.2/sql/create-materialized-view/).

## Stream

To take full advantage of incrementally updated materialized views from
a Java application, instead of [querying](#query) Materialize for the
state of a view at a point in time, use a [`SUBSCRIBE`
statement](/docs/self-managed/v25.2/sql/subscribe/) to request a stream
of updates as the view changes.

To read a stream of updates from an existing materialized view, open a
long-lived transaction with `BEGIN` and use [`SUBSCRIBE` with
`FETCH`](/docs/self-managed/v25.2/sql/subscribe/#subscribing-with-fetch)
to repeatedly fetch all changes to the view since the last query:

<div class="highlight">

``` chroma
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
            stmt.execute("DECLARE c CURSOR FOR SUBSCRIBE counter_sum");
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

</div>

The [`SUBSCRIBE` output
format](/docs/self-managed/v25.2/sql/subscribe/#output) of `rs` is a
`ResultSet` of view updates. When a row of a subscribed view is
**updated,** two objects will show up in the `rows` array:

<div class="highlight">

``` chroma
    ...
    1648567756801 1 value_3
    1648567761801 1 value_4
    1648567785802 -1 value_4
    ...
```

</div>

A `mz_diff` value of `-1` indicates that Materialize is deleting one row
with the included values. An update is just a retraction
(`mz_diff: '-1'`) and an insertion (`mz_diff: '1'`) with the same
timestamp.

## Clean up

To clean up the sources, views, and tables that we created, first
connect to Materialize using a [PostgreSQL
client](/docs/self-managed/v25.2/integrations/sql-clients/) and then,
run the following commands:

<div class="highlight">

``` chroma
DROP MATERIALIZED VIEW IF EXISTS counter_sum;
DROP SOURCE IF EXISTS counter;
DROP TABLE IF EXISTS countries;
```

</div>

## Java ORMs

ORM frameworks like **Hibernate** tend to run complex introspection
queries that may use configuration settings, system tables or features
not yet implemented in Materialize. This means that even if a tool is
compatible with PostgreSQL, it’s not guaranteed that the same
integration will work out-of-the-box.

The level of support for these tools will improve as we extend the
coverage of `pg_catalog` in Materialize and join efforts with each
community to make the integrations Just Work™️.

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/integrations/client-libraries/java-jdbc.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
