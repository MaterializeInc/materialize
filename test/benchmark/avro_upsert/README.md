# Avro Upsert Benchmark

This benchmark tests our ability to ingest Avro Upsert messages. In it's current configuration, it
assumes that Kafka topics should have 30 partitions and that you want 16 worker threads.

## Running the Avro Upsert Test

Download `genavro_400m_v2_out.gz`, unzip it, and copy it into the directory called `data` in the
current directory. Then run:

    ./mzcompose run test-upsert-performance

Navigate to `http://localhost:3000` to see the Kafka ingest performance.

## Analyzing Ingest Performance

Create a Python script that knows how many messages are in the Kafka Topics. Record the initial
time when the benchmark starts. Wait until `mz_kafka_messages_ingested` is equal to the total
number of messages in Kafka. Record the end time. Run a query to get the time-series data for
dataflow events read over time and consumer lag by partition over time.

Print a summary total time taken and messages ingested per second.

Dump these into a format where we can compare them against previous runs.

Initially, only compare runs on the same machine (or machine type). Gather num processors, amount
of memory and other metrics. Gather the host name. Assign each run a unique identifier.

Create a loop to vary parameters of interest, including:

    Number of worker threads **** Do this one first
    Number of unique keys in the dataset
    Number of messages inserted into the topic
    Number of partitions in the topic
    Size of messages, by varying the number of fields in the payload

### Metrics to gather

#### Kafka Messages Ingested

To grab the total number of messages ingested, run:

```
curl 'http://localhost:49226/api/v1/query?query=sum(mz_kafka_messages_ingested)'
```

#### Consumer Lag

```
topk(50, clamp_min(
            mz_kafka_partition_offset_max - mz_kafka_partition_offset_ingested,
    0))
```

#### Messages Ingested by Format

```
rate(mz_dataflow_events_read_total[$__interval])
```
