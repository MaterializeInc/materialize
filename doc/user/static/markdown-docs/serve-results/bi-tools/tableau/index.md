# Tableau

How to create real-time dashboards with Tableau



You can use [Tableau Cloud](https://www.tableau.com/products/cloud-bi) and
[Tableau Desktop](https://www.tableau.com/products/desktop) to create real-time
dashboards based on the data maintained in Materialize.

## Tableau Cloud

### Database connection details

To set up a connection from Tableau Cloud to Materialize, use the native
[PostgreSQL database connector](https://help.tableau.com/current/pro/desktop/en-us/examples_postgresql.htm)
with the following parameters:

Field             | Value
----------------- | ----------------
Server            | Materialize host name.
Port              | **6875**
Database          | **materialize**
Username          | Materialize user.
Password          | App-specific password.
Require SSL       | ✓

For more details and troubleshooting, check the
[Tableau documentation](https://help.tableau.com/current/pro/desktop/en-us/examples_postgresql.htm).

[//]: # "TODO(morsapaes) Clarify minimum refresh rate and details about live
connections"

## Tableau Desktop

### Setup


**macOS:**

To set up a connection from Tableau Desktop to Materialize, you must:

1. Download the [Java 8 JDBC driver for PostgreSQL](https://jdbc.postgresql.org/download/)
1. Copy the `.jar` file to the following directory (which may have to be created manually):

   `~/Library/Tableau/Drivers`



**Linux:**

To set up a connection from Tableau Desktop to Materialize, you must:

1. Download the [Java 8 JDBC driver for PostgreSQL](https://jdbc.postgresql.org/download/)
1. Copy the `.jar` file to the following directory (which may have to be created manually):

   `/opt/tableau/tableau_driver/jdbc`



**Windows:**

To set up a connection from Tableau Desktop to Materialize, you must:

1. Download the [Java 8 JDBC driver for PostgreSQL](https://jdbc.postgresql.org/download/)
1. Copy the `.jar` file to the following directory (which may have to be created manually):

   `C:\Program Files\Tableau\Drivers`




### Database connection details

Once you've set up the required driver, start Tableau and run through the
following steps:

1. On the left side, find the **Connect to a Server** section
1. Select **More** and then **PostgreSQL**
1. Use the following details to configure the connection:

    Field          | Value
    -------------- | ----------------------
    Server         | Materialize host name.
    Port           | **6875**
    Database       | **materialize**
    Authentication | Username and Password
    Username       | Materialize user.
    Password       | App-specific password.
    Require SSL    | ✓

4. Click **Sign In** to connect to Materialize

For more details and troubleshooting, check the
[Tableau documentation](https://help.tableau.com/current/pro/desktop/en-us/examples_postgresql.htm).

[//]: # "TODO(morsapaes) Clarify minimum refresh rate and details about live
connections"

## Configure a custom cluster

<p>To direct queries to a specific cluster, <a href="/sql/alter-role" >set the cluster at the role
level</a> using the following SQL statement:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-sql" data-lang="sql"><span class="line"><span class="cl"><span class="k">ALTER</span><span class="w"> </span><span class="k">ROLE</span><span class="w"> </span><span class="o">&lt;</span><span class="n">your_user</span><span class="o">&gt;</span><span class="w"> </span><span class="k">SET</span><span class="w"> </span><span class="k">CLUSTER</span><span class="w"> </span><span class="o">=</span><span class="w"> </span><span class="o">&lt;</span><span class="n">custom_cluster</span><span class="o">&gt;</span><span class="p">;</span><span class="w">
</span></span></span></code></pre></div><p>Replace <code>&lt;your_user&gt;</code> with the name of your Materialize role and <code>&lt;custom_cluster&gt;</code> with the name of the cluster you want to use.</p>
<p>Once set, all new sessions for that user will automatically run in the specified
cluster, eliminating the need to manually specify it in each query or
connection.</p>


### Troubleshooting

Errors like the following indicate that the JDBC driver was not successfully
installed.

```
ERROR: Expected FOR, found WITH;
Error while executing the query
```

```
ERROR: WITH HOLD is unsupported for cursors;
Error while executing the query
```

The errors occur because Tableau falls back to a legacy PostgreSQL ODBC driver
that does not support connecting to Materialize. Follow the [Setup](#setup)
instructions again and ensure you've downloaded the driver to the correct folder
for your platform.
