# chbench

This is a demonstration of Materialize on [CH-benCHmark]—a mashup of TPC-C and
TPC-H designed to test the speed of analytics queries on a rapidly changing
dataset.

There are several moving pieces to this demo. At the bottom of the stack, we
have a MySQL instance that stores the TPC-C dataset. We connect the
CH-benCHmark's transactional load generator to this MySQL instance, sending a
configurable volume of new orders and such through MySQL. Then, we pipe the
MySQL binlog into Kafka (via Debezium and Kafka Connect, though the details are
not so important), and plug `materialized` into the other end. Then we
install the TPC-H queries into `materialized` as materialized views, and watch
as they magically stay up to date.

The components are orchestrated via [Docker Compose][docker-compose], which
runs each component in a Docker container. Note that this will cause some
overhead on macOS; to measure performance, you'll want to use Linux.


To get started, bring up the Docker Compose containers in the background:

```shell session
$ docker-compose up -d
Creating network "chbench_default" with the default driver
Creating chbench_inspect_1      ... done
Creating chbench_chbench_1      ... done
Creating chbench_cli_1          ... done
Creating chbench_mysql_1        ... done
Creating chbench_materialized_1 ... done
Creating chbench_connector_1    ... done
Creating chbench_zookeeper_1    ... done
Creating chbench_kafka_1        ... done
Creating chbench_connect_1         ... done
Creating chbench_schema-registry_1 ... done
```

If all goes well, you'll have MySQL, ZooKeeper, Kafka, Kafka Connect, and
Materialized running, each in their own container, with Debezium configured to
ship changes from MySQL into Kafka. You won't really be able to tell if things
fail to start up, which seems to happen from time to time, as the output from
Docker Compose only tells you if the *container* successfully booted, not if the
service inside the container actually booted. Your best bet is to assume that
the services came up and proceed with the demo—the logs are too spammy to be
useful in quickly determining whether the service came up successfully.

Now, generate some CH-benCHmark data. You'll only need to do this step once, as
the generated data is stored on a Docker volume that persists until you manually
remove it with `docker volume rm`, so if this is your second time through the
demo, you can skip this step.

```shell session
$ docker-compose run chbench gen --warehouses=1
```

You can generate bigger datasets by using more warehouses. Once the data is
generated, it's time to fire up some load:

```shell session
$ docker-compose run chbench run \
    --dsn=mysql --gen-dir=/var/lib/mysql-files \
    --analytic-threads=0 --transactional-threads=5 --run-seconds=300
Databasesystem:
-initializing
Schema creation:
-succeeded
CSV import:
-succeeded
-check with 1 warehouses succeeded
Additional Preparation:
-succeeded
Wait for threads to initialize:
-all threads initialized
Workload:
-start warmup
-start test
```

Once you see that the CSV import has succeeded, the initial data set has been
loaded into MySQL. It's time to connect to `materialized` and install
some materialized views!

```shell session
$ docker-compose run cli
# TODO(benesch): figure out how to make CREATE SOURCES work.
CREATE SOURCE warehouse FROM 'kafka://kafka:9092/mysql.tpcch.warehouse' USING SCHEMA REGISTRY 'http://schema-registry:8081';
CREATE SOURCE district FROM 'kafka://kafka:9092/mysql.tpcch.district' USING SCHEMA REGISTRY 'http://schema-registry:8081';
CREATE SOURCE customer FROM 'kafka://kafka:9092/mysql.tpcch.customer' USING SCHEMA REGISTRY 'http://schema-registry:8081';
CREATE SOURCE history FROM 'kafka://kafka:9092/mysql.tpcch.history' USING SCHEMA REGISTRY 'http://schema-registry:8081';
CREATE SOURCE neworder FROM 'kafka://kafka:9092/mysql.tpcch.neworder' USING SCHEMA REGISTRY 'http://schema-registry:8081';
CREATE SOURCE order FROM 'kafka://kafka:9092/mysql.tpcch.order' USING SCHEMA REGISTRY 'http://schema-registry:8081';
CREATE SOURCE orderline FROM 'kafka://kafka:9092/mysql.tpcch.orderline' USING SCHEMA REGISTRY 'http://schema-registry:8081';
CREATE SOURCE item FROM 'kafka://kafka:9092/mysql.tpcch.item' USING SCHEMA REGISTRY 'http://schema-registry:8081';
CREATE SOURCE stock FROM 'kafka://kafka:9092/mysql.tpcch.stock' USING SCHEMA REGISTRY 'http://schema-registry:8081';
CREATE SOURCE nation FROM 'kafka://kafka:9092/mysql.tpcch.nation' USING SCHEMA REGISTRY 'http://schema-registry:8081';
CREATE SOURCE supplier FROM 'kafka://kafka:9092/mysql.tpcch.supplier' USING SCHEMA REGISTRY 'http://schema-registry:8081';
CREATE SOURCE region FROM 'kafka://kafka:9092/mysql.tpcch.region' USING SCHEMA REGISTRY 'http://schema-registry:8081';

CREATE VIEW q01 as SELECT
        ol_number,
        sum(ol_quantity) as sum_qty,
        sum(ol_amount) as sum_amount,
        avg(ol_quantity) as avg_qty,
        avg(ol_amount) as avg_amount,
        count(*) as count_order
FROM
        orderline
WHERE
        ol_delivery_d > date '1998-12-01'
GROUP BY
        ol_number;

PEEK q01;
PEEK q01;
PEEK q01;
```

For maximum style points, use `watch-sql` for some live query monitoring:

```bash session
$ docker-compose run cli watch-sql "PEEK q01"
```

[CH-benCHmark]: https://db.in.tum.de/research/projects/CHbenCHmark/index.shtml?lang=en
[docker-compose]: https://docs.docker.com/compose/
