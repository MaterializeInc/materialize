# Sinks

Learn about sinks in Materialize.



## Overview

Sinks are the inverse of sources and represent a connection to an external
stream where Materialize outputs data. You can sink data from a **materialized**
view, a source, or a table.

## Sink methods

To create a sink, you can:


| Method | External system | Guide(s) or Example(s) |
| --- | --- | --- |
| Use <code>COPY TO</code> command | Amazon S3 or S3-compatible storage | <ul> <li><a href="/serve-results/sink/s3/" >Sink to Amazon S3</a></li> </ul>  |
| Use Census as an intermediate step | Census supported destinations | <ul> <li><a href="/serve-results/sink/census/" >Sink to Census</a></li> </ul>  |
| Use <code>COPY TO</code> S3 or S3-compatible storage as an intermediate step | Snowflake and other systems that can read from S3 | <ul> <li><a href="/serve-results/sink/snowflake/" >Sink to Snowflake</a></li> </ul>  |
| Use a native connector | Kafka/Redpanda | <ul> <li><a href="/serve-results/sink/kafka/" >Sink to Kafka/Redpanda</a></li> </ul>  |
| Use <code>SUBSCRIBE</code> | Various | <ul> <li><a href="https://github.com/MaterializeInc/mz-catalog-sync" >Sink to Postgres</a></li> <li><a href="https://github.com/MaterializeIncLabs/mz-redis-sync" >Sink to Redis</a></li> </ul>  |


## Clusters and sinks

Avoid putting sinks on the same cluster that hosts sources.

See also [Operational guidelines](/manage/operational-guidelines/).

## Hydration considerations

During creation, Kafka sinks need to load an entire snapshot of the data in
memory.

## Related pages

- [`CREATE SINK`](/sql/create-sink)
