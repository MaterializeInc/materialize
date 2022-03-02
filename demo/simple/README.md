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
documentation](https://materialize.com/docs/overview/architecture)––the only
components not accounted for there are:

- Kafka's infrastructure (e.g. Zookeeper)
- The MySQL load generator in this demo

## What to Expect

Our load generator (`simple-loadgen`) populates MySQL with 3 tables: `region`,
`user`, and `purchase`.

![simple_demo_schema](https://user-images.githubusercontent.com/23521087/136808547-9aa91293-3a03-4bf2-99d9-5ca220a21781.png)

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
    $ ./mzcompose up -d
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
    ./mzcompose run cli
    ```

1. Now that you're in the Materialize CLI (denoted by the terminal prefix
   `mz>`), define all of the tables in `mysql.simple` as Kafka sources in
   Materialize.

    ```sql
    CREATE SOURCE purchase
    FROM KAFKA BROKER 'kafka:9092' TOPIC 'mysql.simple.purchase'
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081'
    ENVELOPE DEBEZIUM;

    CREATE SOURCE region
    FROM KAFKA BROKER 'kafka:9092' TOPIC 'mysql.simple.region'
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081'
    ENVELOPE DEBEZIUM;

    CREATE SOURCE user
    FROM KAFKA BROKER 'kafka:9092' TOPIC 'mysql.simple.user'
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081'
    ENVELOPE DEBEZIUM;
    ```

1. Create a view for Materialize to maintain. In this case, we'll keep a sum of
   all purchases made by users in each region:

    ```sql
    CREATE MATERIALIZED VIEW purchase_sum_by_region AS
        SELECT sum(purchase.amount) AS region_sum, region.id AS region_id
        FROM purchase
        INNER JOIN user ON purchase.user_id = user.id
        INNER JOIN region ON region.id = user.region_id
        GROUP BY region.id;
    ```

1. Read the materialized view.

    ```sql
    SELECT * FROM purchase_sum_by_region;
    ```

    Go ahead and do that a few times; you should see the `region_sum` continue
    to increase for all `region_id`s.

1. Close out of the Materialize CLI (<kbd>Ctrl</kbd> + <kbd>D</kbd>).

1. Watch the report change using the `watch-sql` container, which continually
   streams changes from Materialize to your terminal.

    ```shell
    ./mzcompose run cli watch-sql "SELECT * FROM purchase_sum_by_region"
    ```

1. Once you're sufficiently wowed, close out of the `watch-sql` container
   (<kbd>Ctrl</kbd> + <kbd>D</kbd>), and bring the entire demo down.

    ```shell
    ./mzcompose down
    ```

### Troubleshooting

If you encounter any issues, such as not being able to create sources in
Materialize or `purchase_sum_by_region` being empty, go through the following
steps.

1. Launch MySQL instance.

    ```shell
    ./mzcompose run mysqlcli
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
`./mzcompose logs -f connector`.

If you don't see values here, there is likely an issue with the load generator;
check its logs using `./mzcompose logs loadgen`.
