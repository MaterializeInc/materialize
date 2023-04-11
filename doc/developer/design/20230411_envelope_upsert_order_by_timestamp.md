- Feature name: Envelope upsert with order by timestamp (limited scope)
- Associated: https://github.com/MaterializeInc/materialize/issues/16512

# Summary

- Support user specified order by in Kafka upsert sources in a limited capacity. If users want to specify custom ordering which is semantically meaningful to them, there's currently no easy way to do that. They could potentially use `ENVELOPE NONE` along with `DISTINCT ON` to get similar results, but it's cumbersome and lacks the automatic compaction on keys with normal `ENVELOPE UPSERT`.
- Currently, will limit the scope to allow only `ORDER BY` the `TIMESTAMP` metadata field behind an unsafe flag. Depending upon user feedback this can later be expanded to support expressions and extractions from other columns like `HEADERS`.

# Scope

The scope will be limited to only allow `ORDER BY TIMESTAMP` (default ordering `ASC`).

- For kafka upsert style topics, value = NULL is taken to mean that it’s a delete tombstone and that the corresponding key should be removed. If we want to keep the same behavior it would make sense to limit the `ORDER BY` columns to only the metadata columns.
- Limiting it to only `TIMESTAMP` because
    - `OFFSET` is already default. We can make it explicit though if we want
    - `PARTITION`, `KEY`, `TOPIC`, would not make a good order by clause
    - `HEADERS` by itself would not also make a good order by clause (it’s a json blob), but later we can probably allow expressions so that values could be extracted from it.
- Limiting to only `ASC` as default, because for compaction it makes sense to take semantically the _latest_ value of something, which implies an `ASC` order by

# Explanation

## Proposal

Make it possible with unsafe-mode to provide custom ORDER BY clause which will override the default ordering by offset. The given ORDER BY identifier should refer to the included TIMESTAMP metadata column.

Examples of valid syntax:
- `CREATE SOURCE ... INCLUDE TIMESTAMP ENVELOPE UPSERT ( ORDER BY TIMESTAMP )`
- `CREATE SOURCE ... INCLUDE TIMESTAMP AS ts ENVELOPE UPSERT ( ORDER BY ts )`

Examples of invalid syntax:
- `CREATE SOURCE ... INCLUDE OFFSET ENVELOPE UPSERT ( ORDER BY TIMESTAMP )` (TIMESTAMP column is not included)
- `CREATE SOURCE ... INCLUDE PARTITION AS p ENVELOPE UPSERT ( ORDER BY p )` (ORDER BY identifier does not refer to the TIMESTAMP metadata column)

# Reference Explanation
For envelope upsert, the included metadata columns are appended to the value and eventually persisted. For this feature, we will keep track of the index of the order by field and read the corresponding value to compare when upsert-ing.

Here's an example of the upsert behavior where the source consists of a key, value and the timestamp from metadata and is ordered by the timestamp.

```
 Previously persisted data        Incoming kafka stream decoded
+------------------------+         +-------------------------+
| Ok (key1, value1, 100) |         |  Ok (key1, value3, 200) |
| Ok (key2, value2, 201) |         |  Err(key3, some_error2) |
| Err(key3, some_error1) |         |  Ok (key2, value4, 200) |
+------------------+-----+         +-----+-------------------+
                   |                     |
                   |                     |
                   |                     |
                   |                     |
                   v    Upsert Output    v
                  +------------------------+
                  | Ok (key1, value3, 200) |
                  | Ok (key2, value2, 201) |
                  | Err(key3, some_error2) |
                  +------------------------+
```
Note: As shown in the example above, the errors are still implicitly ordered by offset as we do not persist any extra metadata separately. A later error with the same key will always overwrite a previous one.

# Rollout
This feature will be rolled out behind the unsafe flag.

## Testing and Observability

The custom ordering can be tested via testdrive tests in the CI.

## Lifecycle

What will it take to promote this to the next stage i.e. Alpha?

- We should work with some users and see how useful this is to them. This would also give us a better idea on how to expand this feature later on.
- Some questions to give us a better idea
    - How is their data structured and what columns will they want to use for the custom order by?
    - What kind of compaction are they using?

# Drawbacks

Users will need to be careful of what kind of compaction they have for the kafka topic. The upsert will do the correct thing based on the data it sees.

If old data has been compacted away either via time based or key based compaction, and we start ingesting the source after that, the result of upsert might not be accurate.

If users actually are using this custom order by, it’s likely they are **not** using key based compaction with Kafka because semantically that would be upsert-ing on offset.

# Conclusion and alternatives

An alternative could be waiting for `TRANSFORM USING` which has a much broader scope and could cover this particular scenario. In the meantime, having a smaller scoped custom order by could be useful for interested users.

# Future work

Depends upon user feedback to see how this should be expanded.
