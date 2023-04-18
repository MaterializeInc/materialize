- Feature name: Envelope upsert with order by timestamp (limited scope)
- Associated: https://github.com/MaterializeInc/materialize/issues/16512

# Summary

- Support user specified order by in Kafka upsert sources in a limited capacity. If users want to specify custom ordering which is semantically meaningful to them, there's currently no easy way to do that. They could potentially use `ENVELOPE NONE` along with a `DISTINCT ON` view to get similar results, but it's cumbersome and lacks the automatic compaction on keys with normal `ENVELOPE UPSERT`.
- Currently, will limit the scope to allow only `ORDER BY` the `TIMESTAMP` and the `OFFSET` metadata fields. Depending upon user feedback this can later be expanded to support expressions and extractions from other columns like `HEADERS`.

# Scope

The scope will be limited to only allow `ORDER BY ( TIMESTAMP, OFFSET )` (ascending order).

- In a key-compacted Kafka topic, value = NULL is taken to mean that it’s a delete tombstone and that the corresponding key should be removed. If we want to borrow that tombstone semantic for our more flexible upsert source, it would make sense to limit the `ORDER BY` columns to only the metadata columns.
- Limiting it to only `TIMESTAMP` and `OFFSET` because
    - `OFFSET` is already default. We are making it explicit here and it's going to be required that the user always mention `OFFSET` as one of the columns in ORDER BY.
    - `PARTITION`, `KEY`, `TOPIC`, would not make for meaningful orderings:
        - The `PARTITION` of a record is typically derived from its key, and so is the same for every record with a given key.
        - `KEY` is, by definition, the same for every record with a given key.
        - `TOPIC` is the same for every record in the topic.
    - `HEADERS` by itself would not also make a good order by clause (it’s a json blob), but later we can probably allow expressions so that values could be extracted from it.
- Limiting to only ascending order because no users have expressed a need for descending.

# Explanation

## Proposal

Make it possible with unsafe-mode to provide custom ORDER BY clause which will override the default ordering by offset. The given ORDER BY identifiers should refer to only the included TIMESTAMP and OFFSET metadata columns. It will be required that the user always mention OFFSET as one of the columns for the ORDER BY clause.

The option will be part of the `ENVELOPE UPSERT` clause with the following grammar:

`ENVELOPE UPSERT [(ORDER BY (<expr>) [ASC])]`

The `ASC` modifier is optional noise for specifying ascending ordering, for symmetry with the `ORDER BY` clause in `SELECT` statements.

Examples of valid syntax and semantics:
- `CREATE SOURCE ... INCLUDE TIMESTAMP ENVELOPE UPSERT ( ORDER BY ( TIMESTAMP, OFFSET ) ASC )`
- `CREATE SOURCE ... INCLUDE TIMESTAMP AS ts, OFFSET AS o ENVELOPE UPSERT ( ORDER BY ( ts, o ) )`
- `CREATE SOURCE ... INCLUDE OFFSET AS o ENVELOPE UPSERT ( ORDER BY ( o ) )` (equivalent to default ordering by offset behavior)

Examples that are syntactically invalid:
- `CREATE SOURCE ... INCLUDE TIMESTAMP ENVELOPE UPSERT ( ORDER BY TIMESTAMP, OFFSET )` (missing parentheses around order by columns)
- `CREATE SOURCE ... INCLUDE TIMESTAMP AS ts, OFFSET AS o ENVELOPE UPSERT ( ORDER BY ( ts, o ) DESC )` (DESC ordering is not allowed, will be rejected at parsing)

Examples that are syntactically valid but semantically invalid:
- `CREATE SOURCE ... INCLUDE OFFSET ENVELOPE UPSERT ( ORDER BY ( TIMESTAMP ) )` (OFFSET is not specified as one of the order by columns)
- `CREATE SOURCE ... INCLUDE OFFSET ENVELOPE UPSERT ( ORDER BY ( TIMESTAMP, OFFSET ) )` (TIMESTAMP column is not included)
- `CREATE SOURCE ... INCLUDE PARTITION AS p ENVELOPE UPSERT ( ORDER BY ( p ) )` (ORDER BY identifier does not refer to the TIMESTAMP or OFFSET metadata column)

# Reference Explanation
For envelope upsert, the included metadata columns are appended to the value and eventually persisted. For this feature, we will keep track of the indices of the order by fields and read the corresponding values to compare when upsert-ing.

Here's an example of the upsert behavior where the source consists of a key, value, timestamp and offset from metadata and is ordered by (timestamp, offset).

```
 Previously persisted data        Incoming kafka stream decoded
+-------------------------+         +--------------------------+
| Ok (key1, old1, 100, 1) |         |  Ok (key1, new1, 200, 5) |
| Ok (key2, old2, 201, 2) |         |  Ok (key2, new2, 200, 6) |
| Err(key3, some_error1)  |         |  Err(key3, some_error2)  |
| Ok (key4, old4, 300, 4) |         |  Ok (key4, new4, 300, 7) |
+------------------+------+         +-----+--------------------+
                   |                     |
                   |                     |
                   |                     |
                   |                     |
                   v    Upsert Output    v
                  +-------------------------+
                  | Ok (key1, new1, 200, 5) | // value updated, (200, 5) > (100, 1)
                  | Ok (key2, old2, 201, 2) | // old value kept, (200, 6) < (201, 2)
                  | Err(key3, some_error2)  | // new error always overrides old
                  | Ok (key4, new4, 300, 7) | // value updated, (300, 7) > (300, 4)
                  +-------------------------+
```
Since offset is always going to be part of the order by value, it will function as an explicit tie-breaker when timestamps are same.

Note: As shown in the example above, the errors are still implicitly ordered by offset as we do not persist any extra metadata for them separately. A later error with the same key will always overwrite a previous one. This would still allow us to retract errors in the same way as before.

# Rollout
This feature will be rolled out behind the unsafe flag, then upgraded to only being available behind a LaunchDarkly flag.

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

If old data has been compacted away either via time based or key based compaction, and we start ingesting the source after that, the result of upsert will not include the data Kafka has compacted away, which might violate user expectations.

If users actually are using this custom order by, it’s likely they are **not** using key based compaction with Kafka because semantically that would be upsert-ing on offset.

# Conclusion and alternatives

An alternative could be waiting for `TRANSFORM USING` which has a much broader scope and could cover this particular scenario. In the meantime, having a smaller scoped custom order by could be useful for interested users.

# Future work

Depending upon user feedback this feature can be expanded to support more use cases.
- Support extracting custom fields from the HEADERS metadata column and use it to order by.
- Support user provided columns for the ordering, but we will have to figure out the behavior for tombstones which is not resolved.
