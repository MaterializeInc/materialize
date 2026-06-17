# Reproducer: NaN-bearing JSONB crashes the JSON sink encoder.
#
# `strconv::parse_numeric` accepts positive quiet NaN. `Jsonb::from_str`
# recognises the magic `$serde_json::private::Number` map key and calls
# `parse_numeric` on the payload (see src/repr/src/adt/jsonb.rs), so a
# user-supplied JSON text with that magic key smuggles `Datum::Numeric(NaN)`
# past the `cast_jsonbable_to_jsonb` defence
# (src/expr/src/scalar/func/impls/jsonb.rs:159) that the rest of the system
# relies on.
#
# Once stored, sinks reach this datum via `JsonbRef::to_serde_json`
# (src/repr/src/adt/jsonb.rs:213), which calls `serde_json::to_value(...)`
# on a writer that produces the literal token `NaN`. `serde_json::to_value`
# rejects that token as "invalid number"; the `.expect("conversion to
# serde_json::Value known to be valid")` panics the calling thread.
#
# Used by:
#   * src/interchange/src/json.rs:161           (Kafka JSON sink)
#   * src/interchange/src/avro/encode.rs:366    (Kafka Avro sink)
#   * src/arrow-util/src/builder.rs:926, 929    (COPY TO PARQUET / Iceberg)
#
# Expected behaviour after the fix: either `'...'::jsonb` rejects positive
# NaN, or the sink encoder substitutes the string "NaN" the way
# `cast_jsonbable_to_jsonb` already does for the SQL cast path.

$ set-arg-default default-storage-size=scale=1,workers=1

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE MATERIALIZED VIEW nan_jsonb_view AS
  SELECT '{"$serde_json::private::Number": "NaN"}'::jsonb AS j;

> SELECT j::text FROM nan_jsonb_view;
NaN

> CREATE CLUSTER nan_sink_cluster SIZE '${arg.default-storage-size}';

> CREATE SINK nan_sink
  IN CLUSTER nan_sink_cluster
  FROM nan_jsonb_view
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-jsonb-nan-${testdrive.seed}')
  FORMAT JSON
  ENVELOPE DEBEZIUM

$ kafka-verify-data format=json sink=materialize.public.nan_sink key=false
{"before":null,"after":{"j":"NaN"}}