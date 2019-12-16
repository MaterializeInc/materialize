# Metabase demo instructions

The purpose of this demo is to visualize Materialize queries and
highlight the ease of connecting to BI (business intelligence) tools.


The Metabase demo can be run:
1. Manually on your laptop
1. Using Docker
1. On AWS

For now, these instructions only describe how to run the Metabase demo
with Docker, although many steps are the same between the three.

## Docker

### Starting the Docker containers and loading data

First, we use [this script](https://github.com/MaterializeInc/materialize/blob/master/ex/chbench/dc.sh)
to pull the necessary Docker images, bring containers up with Docker compose, and
load an initial set of data. To do that, we run:

```console
$ ./dc.sh up :init:
$ ./dc.sh up :demo:
```

Note: Running :init: loads an initial set of TPC-CH data into Materialize.
Running :demo: will  load that same data in Materialize again. In the future,
we should find a way to load data/initialize tables without creating these duplicates.

Now that the data is loaded into Materialize, we want to create sources
with it. Enter a psql shell connected to Materialize:

```console
psql -h localhost -p 6875 gssencmode=disable\ sslmode=disable
```

And create the sources:

```console
CREATE SOURCES LIKE 'mysql.tpcch.%' FROM 'kafka://kafka:9092' USING SCHEMA REGISTRY 'http://schema-registry:8081';
```

### Connecting Metabase to Materialize

Starting up the required Docker containers using `dc.sh` brings up the Metabase
and Materialize services. Now, we need to connect the two.

To open Metabase, go to `localhost:3030` in your browser.
You should see Metabase is already running, but needs you to
create a user and connect to a database. Note: Each time you
restart the Metabase container, you will need to create a
user and reconnect to Materialize.

To create a user, simply fill in the user information fields.
This information is not important, although you may be asked to log in
at another point in the future.

To connect to Materialize, provide Metabase with the following
host and port information:
- Host: materialized (the name of the Materialize Docker service)
- Port: 6875

All of the other fields Metabase requests to connect to the database
(such as username and password), are arbitrary for the time being.

Next, you will either agree/disagree to send analytics back to Metabase.
You will be able to continue through either way.

You should now be able to see the main Metabase dashboard.


### Configuring Metabase

Once you have connected Metabase to Materialize and you are able to
see the main dashboard, you will want to sync the Metabase and
Materialize database schemas. To do that, you should:
1. Navigate to `admin/databases` in your browser
2. Click on the database you created
3. Click "sync database schema now" in the top right
4. Go back to the `/` path

Now that Metabase is connected to Materialize, we can create a demo dashboard.
To do this, you will have to:
- Create a new dashboard
- Create questions (queries) in Metabase
- Add questions to the dashboard


#### Create a new dashboard
From the `/` path, you should be able to create a dashboard by clicking the
`+` button in the top right hand corner.


#### Create questions
From the `/` path, you should be able to create a question by clicking the
"Ask a Question" button in the top right hand corner, taking you to
`/question/new`.

To demo Materialize, we have been adding two copies of a handful of
queries to a demo dashboard: one copy without a view and one copy
with a view. Note: You have to manually create these views in order
for the second query to work, either through your `psql` shell or
the Metabase interactive SQL editor.

- A Simple count:
    - Basic query:
      ```sql
      SELECT count(*) AS "count"
      FROM "mysql_tpcch_orderline";
      ```
    - Create the view:
      ```sql
      CREATE VIEW orderline_count AS
      SELECT count(*) FROM mysql_tpcch_orderline;
      ```
    - Use the view:
      ```sql
      SELECT * FROM orderline_count;
      ```

- Q01:
    - Basic query:
      ```sql
        SELECT
            ol_number,
            sum(ol_quantity) AS sum_qty,
            sum(ol_amount) AS sum_amount,
            avg(ol_quantity) AS avg_qty,
            avg(ol_amount) AS avg_amount,
            count(*) AS count_order
        FROM mysql_tpcch_orderline
        WHERE ol_delivery_d > TIMESTAMP '2007-01-02 00:00:00.000000'
        GROUP BY ol_number
        ORDER BY ol_number;
      ```
    - Create the view:
      ```sql
        CREATE VIEW q01 AS
        SELECT
            ol_number,
            sum(ol_quantity) AS sum_qty,
            sum(ol_amount) AS sum_amount,
            avg(ol_quantity) AS avg_qty,
            avg(ol_amount) AS avg_amount,
            count(*) AS count_order
        FROM mysql_tpcch_orderline
        WHERE ol_delivery_d > TIMESTAMP '2007-01-02 00:00:00.000000'
        GROUP BY ol_number
        ORDER BY ol_number;
      ```
    - Use the view:
      ```sql
        SELECT * FROM q01;
      ```

More queries can be found and added from the appendix.


#### Add questions to the dashboard

## Appendix: TPC-CH Queries

- Query 2
    ```sql
    SELECT su_suppkey, su_name, n_name, i_id, i_name, su_address, su_phone, su_comment
    FROM
        mysql_tpcch_item,
        mysql_tpcch_supplier,
        mysql_tpcch_stock,
        mysql_tpcch_nation,
        mysql_tpcch_region,
        (
            SELECT
                s_i_id AS m_i_id,
                min(s_quantity) AS m_s_quantity
            FROM
                mysql_tpcch_stock,
                mysql_tpcch_supplier,
                mysql_tpcch_nation,
                mysql_tpcch_region
            WHERE s_su_suppkey = su_suppkey
            AND su_nationkey = n_nationkey
            AND n_regionkey = r_regionkey
            AND r_name like 'EUROP%'
            GROUP BY s_i_id
        ) m
    WHERE i_id = s_i_id
    AND s_su_suppkey = su_suppkey
    AND su_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND i_data like '%b'
    AND r_name like 'EUROP%'
    AND i_id = m_i_id
    AND s_quantity = m_s_quantity
    ORDER BY n_name, su_name, i_id;
    ```
- Query 3
    ```sql
    SELECT ol_o_id, ol_w_id, ol_d_id, sum(ol_amount) AS revenue, o_entry_d
    FROM
        mysql_tpcch_customer,
        mysql_tpcch_neworder,
        mysql_tpcch_order,
        mysql_tpcch_orderline
    WHERE
        c_state LIKE 'A%'
        AND c_id = o_c_id
        AND c_w_id = o_w_id
        AND c_d_id = o_d_id
        AND no_w_id = o_w_id
        AND no_d_id = o_d_id
        AND no_o_id = o_id
        AND ol_w_id = o_w_id
        AND ol_d_id = o_d_id
        AND ol_o_id = o_id
        AND o_entry_d > TIMESTAMP '2007-01-02 00:00:00.000000'
    GROUP BY ol_o_id, ol_w_id, ol_d_id, o_entry_d
    ORDER BY revenue DESC, o_entry_d;
    ```
