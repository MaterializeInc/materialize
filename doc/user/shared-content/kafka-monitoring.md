In Materialize, Kafka sources store the ingestion progress metadata (previously,
the metadata was stored in the `<source>_progress`). The following metadata is
available for each source:

Field          | Type                                     | Meaning
---------------|------------------------------------------|--------
`partition`    | `numrange`                               | The upstream Kafka partition.
`offset`       | [`uint8`](/sql/types/uint/#uint8-info)   | The greatest offset consumed from each upstream Kafka partition.

You can monitor the source progress via the [Materialize
Console](/console/monitoring/).  Alternatively, you can query the source:

```mzsql
SELECT
  partition, "offset"
FROM
  (
    SELECT
      -- Take the upper of the range, which is null for non-partition rows
      -- Cast partition to u64, which is more ergonomic
      upper(partition)::uint8 AS partition, "offset"
    FROM
      <src_name>
  )
WHERE
  -- Remove all non-partition rows
  partition IS NOT NULL;
```

As long as any offset continues increasing, Materialize is consuming data from
the upstream Kafka broker. For more details on monitoring source ingestion
progress and debugging related issues, see
[Troubleshooting](/ops/troubleshooting/).
