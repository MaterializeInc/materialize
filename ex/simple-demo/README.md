# Simple Demo

This simple demo aims to show what Materialize feels like for end users of its
direct SQL output. To accomplish this, it launches all of the requisite
infrastructure (MySQL, Debezium, Kafka, etc.), as well as a MySQL load
generator. From there, you can launch the Materialize CLI, define sources and
materialized views, and watch it maintain those views in real time.

To simplify deploying all of this infrastructure, the demo is enclosed in a
series of Docker images glued together via Docker Compose. As a secondary
benefit, you can run the demo via Linux, an EC2 VM instance, or a Mac laptop.

For a better sense of what this deployment looks like, see our [architecture
doc](../../doc/architecture.md)––the only components not accounted for there
are:

- Kafka's infrastructure (e.g. Zookeeper)
- Our MySQL load generator

## What to Expect

Our load generator (`simple-loadgen`) populates MySQL with 3 tables: `region`,
`user`, and `purchase`.

![simple demo schema](../../doc/img/simple_demo_schema.png)

The database gets seeded with regions, users in those regions, and the purchases
those users make. After seeding, users continue making purchases (~10/second for
~15 minutes).

As these writes occur, Debezium/Kafka stream the changes out of MySQL.
Materialize subscribes to this change feed and maintains our materialized views
with the incoming data––materialized views typically being some report whose
information we're regularly interested in viewing. 

For example, if we wanted real time statistics of total sales by region,
Materialize could maintain that report as a materialized view. And, in fact,
that is exactly what this demo will show.

## Prepping Mac Laptops

If you're on a Mac laptop, you might want to increase the amount of memory
available to Docker Engine.

1. From the Docker Desktop menu bar app, select **Preferences**.
1. Go to the **Advanced** tab.
1. Select at least **8 GiB** of **Memory**.
1. Click **Apply and Restart**.

## Running the Demo

1. Bring up the Docker Compose containers in the background:

    ```shell session
    $ docker-compose up -d
    Creating network "demo_default" with the default driver
    Creating demo_inspect_1      ... done
    Creating demo_chbench_1      ... done
    Creating demo_cli_1          ... done
    Creating demo_mysql_1        ... done
    Creating demo_materialized_1 ... done
    Creating demo_connector_1    ... done
    Creating demo_zookeeper_1    ... done
    Creating demo_kafka_1        ... done
    Creating demo_connect_1         ... done
    Creating demo_schema-registry_1 ... done
    ```

    If all goes well, you'll have MySQL, ZooKeeper, Kafka, Kafka Connect,
    Materialize, and a load generator running, each in their own container, with
    Debezium configured to ship changes from MySQL into Kafka.

1. Launch the Materialize CLI.

    ```shell session
    docker-compose run cli
    ```

1. Now that you're in the Materialize CLI (denoted by the terminal prefix
   `mz>`), define all of the tables in `mysql.simple` as Kafka sources in
   Materialize.

    ```sql
    CREATE SOURCES 
    LIKE 'mysql.simple.%' 
    FROM 'kafka://kafka:9092' 
    USING SCHEMA REGISTRY 'http://schema-registry:8081';
    ```

    You'll see the name of the sources you created, which should exactly match
    this:

    ```
    +-----------------------+
    | Topic                 |
    |-----------------------|
    | mysql_simple_user     |
    | mysql_simple_purchase |
    | mysql_simple_region   |
    +-----------------------+
    ```

    If the above command doesn't generate any topics, wait a few seconds and run
    the command again. If it doesn't work after ~30 seconds, check the
    **Troubleshooting** section below.

1. Create a view for Materialize to maintain. In this case, we'll keep a sum of
   all purchases made by users in each region:

    ```sql
    CREATE VIEW purchase_sum_by_region
    AS
        SELECT  sum(purchase.amount) AS region_sum, 
                region.id AS region_id
        FROM mysql_simple_purchase AS purchase
        INNER JOIN mysql_simple_user AS user
            ON purchase.user_id = user.id
        INNER JOIN mysql_simple_region AS region
            ON region.id = user.region_id
        GROUP BY region.id;
    ```

1. Peek the materialized view.

    ```sql
    PEEK purchase_sum_by_region;
    ```

    Go ahead and do that a few times; you should see the `region_sum` continue
    to increase for all `region_id`s.

1. Close out of the Materialize CLI (_ctrl + d_).

1. Watch the report change using the `watch-sql` container, which continually
   streams changes from Materialize to your terminal.

    ```shell
    docker-compose run cli watch-sql "PEEK purchase_sum_by_region"
    ```

1. Once you're sufficiently wowed, close out of the `watch-sql` container (_ctrl
   + d_), and bring the entire demo down.

    ```shell
    docker-compose down
    ```

### Troubleshooting

If you encounter any issues, such as not being able to create sources in
Materialize or `purchase_sum_by_region` being empty, go through the following
steps.

1. View the sources that are available in Materialize.

    ```shell
    docker-compose run cli
    ```
    ```sql
    SHOW SOURCES;
    ```

    If you see `mysql_simple_...` sources, you might have run into issues
    because you've had a working instance running in the past. You can try to
    resolve this by closing out of the Materialize CLI (`ctrl + d`), and then
    bringing the demo down (`docker compose down`).

1. Launch MySQL instance.

    ```shell
    docker-compose run mysqlcli
    ```

1. Run the query that the view is based on directly within MySQL.

    ```sql
    USE simple;
    SELECT  sum(purchase.amount) AS region_sum, 
            region.id AS region_id
    FROM purchase
    INNER JOIN user
        ON purchase.user_id = user.id
    INNER JOIN region
        ON region.id = user.region_id
    GROUP BY region.id;
    ```

If you see values here, there is likely an issue with Debezium or Kafka
streaming the values out of MySQL; check the Kafka connector logs using
`docker-compose logs -f connector`.

If you don't see values here, there is likely an issue with the load generator;
check its logs using `docker-compose logs loadgen`.

### Caveats and FAQ

- Materialize cannot currently order the results of the materialized view. This
  is a known limitation and will be fixed in a future build.
- Materialize requires that the load generator perform some updates on the
  `region` and `user` table after the source has been created so that it picks
  up that all of these records exist. This is a known limitation and will be
  fixed in a future build.
