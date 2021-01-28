# Avro Upsert Benchmark

This benchmark tests our ability to ingest Avro Upsert messages. In it's current configuration, it
assumes that Kafka topics should have 30 partitions and that you want 16 worker threads.

## Running the Avro Upsert Test

Download `genavro_400m_v2_out.gz`, unzip it, and copy it into the directory called `data` in the
current directory. Then run:

    ./mzcompose run test-upsert-performance

Navigate to `http://localhost:3000` to see the Kafka ingest performance.
