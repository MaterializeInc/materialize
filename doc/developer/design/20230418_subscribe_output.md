- Feature name: Subscribe outputs
- Associated: [#10593](https://github.com/MaterializeInc/materialize/issues/10593)

# Summary
[summary]: #summary

Clients have had trouble using subscribe. They've struggled
reconciling the current output of subscribe, a list of inserts and
deletes of tuples, with their application-level data. To
bridge that gap we introduce three ways to change how subscribe
produces its output: `WITHIN TIMESTAMP ORDER BY`, `ENVELOPE UPSERT`,
and `ENVELOPE DEBEZIUM`. These output types are simple to implement
because they operate on the diffs present in a single timestamp. We
hope they'll match the client's mental model of updates on their results.

# Motivation
[motivation]: #motivation

Sometimes clients want to think of the output of subscribe as a list of
updates to the underlying relation. While the current output reflects
the changes to the underlying relation, the lack of consistent
ordering has troubled some clients. They worry about handling a case
like `(k, v2, +1), (k, v1, -1)` where an update appears as an insert
for the new value for a particular key occurs followed by the deletion
of its previous value. The output types proposed all solve this
problem with varying degrees of flexibility. `ENVELOPE UPSERT` will
transform the output of `SUBSCRIBE` into a series of `UPSERTS`;
`ENVELOPE DEBEZIUM` will transform the output into a series of
debezium updates with a `before` and `after` state. When viewed as
upserts the ordering of retractions and insertions doesn't
matter. `WITHIN TIMESTAMP ORDER BY` is the most flexible: the client
can specify a set of columns to order the output by (within each
timestamp), including `mz_diff`. They can use this ordering to force
retractions to occur before additions in order to simplify the client
side handling.

For the simple case of a one to one mapping between keys and values
(there's at most one value at a time for each key), any of these
output types would let the client see the changes to the underlying
relation as a series of upserts. `WITHIN TIMESTAMP ORDER BY` could
also be used to sort the results from `SUBSCRIBE`; we can think of the
results from `SUBSCRIBE` are naturally ordered by `mz_timestamp`, we
could supply a secondary order. I don't think there are many real use
cases that would benefit from this.


# Explanation
[explanation]: #explanation

`WITHIN TIMESTAMP ORDER BY` takes a `ORDER BY` expression and sorts
the returned data within each distinct timestamp. This `ORDER BY` can
take any column in the underlying object or query in addition to the
`mz_diff` column. A common use case of recovering upserts to key-value
data would be sort by all the keys and `mz_diff`. Since `SUBSCRIBE`
guarantees that there won't be multiple updates for the same row
(rephrased: the batch is already consolidated), this will produce data
that looks like `(k, v1, -1), (k, v2, +1)` which are easier to handle
for the client. If PROGRESS is set, progress messages are unaffected.

Example:
```sql
> CREATE TABLE table(c1 int, c2 int, c3 text)
> SUBSCRIBE table WITHIN TIMESTAMP ORDER BY c1, c2 DESC NULLS LAST, mz_diff
mz_timestamp | mz_diff | c1            | c2   | c3
-------------|---------|---------------|------|-----
100          | +1      | 1             | 20   | foo
100          | -1      | 1             | 2    | bar
100          | +1      | 1             | 2    | boo
100          | +1      | 1             | 0    | data
100          | -1      | 2             | 0    | old
100          | +1      | 2             | 0    | new
```

`ENVELOPE UPSERT`: Takes a list of columns to interpret as a key and
within each distinct timestamp interprets the updates as a series of
upserts. The output columns are reordered as `(mz_timestamp, mz_state,
k1, k2, .., v1, v2, ..)`. There is no `mz_diff` column. The values
`v1, v2, ..` are set the last value if there's an update or an insert
and are all set to `NULL` if the only updates to the key in that
timestamp were deletions. There should not be multiple values for a
given key. If materialize detects that case (which can only reasonably
happen if there are multiple updates for the same key in a given
timestamp), materialize will emit an update a "key violation"
state. `mz_state` is either `"delete"`, `"upsert"`, or `"key
violation"` to represent these three cases.

If PROGRESS is set we also return `mz_progressed` as usual before
`mz_state` and each progress message nulls out all the key and value
columns.

Example:
```sql
> CREATE TABLE kv_store(key int, value int, UNIQUE(key))
> INSERT INTO kv_store VALUES (1, 2), (2, 4)
> SUBSCRIBE kv_store ENVELOPE UPSERT (KEY (key))
mz_timestamp | mz_state       | key  | value
-------------|----------------|------|--------
100          | upsert         | 1    | 2
100          | upsert         | 2    | 4

...
-- at time 200, update key=1's value to 10
...

200          | upsert         | 1    | 10


...
-- at time 300, add a new row with key=3, value=6
...

300          | upsert         | 3    | 6

...
-- at time 400, delete all rows
...

400          | delete         | 1    | NULL
400          | delete         | 2    | NULL
400          | delete         | 3    | NULL

-- at time 500, introduce a key violation for key=1
...

500          | key violation  | 1    | NULL

```

`ENVELOPE DEBEZIUM`: Similarly to `ENVELOPE UPSERT`, takes a list of
columns to interpret as a key and within each distinct timestamp emits
debezium upserts with a `before` and `after` state. The output columns
are reordered as `(mz_timestamp, mz_state, k1, k2, .., before_v1,
before_v2, .., after_v1, after_v2, ..)`. There is no `mz_diff`
column. When there is an addition, `mz_state` is set to `"insert"` and
the `before` values are all set to `NULL`. When there is an update,
`mz_state` is set to `"upsert"`, `before` has the previous contents,
and `after` has the current values. When there is a deletion,
`mz_state` is set to `"delete"`, the `before` values are set to the
previous values and the `after` values are all `NULL`. If it's
detected that there are multiple values for a given key, `mz_state` is
set to `"key violation"` and `before` and `after` are both all set to
`NULL`.

Similar to `ENVELOPE UPSERT`, `ENVELOPE DEBEZIUM` should not be used
when there can be more than one live value per key.

If PROGRESS is set, the key, `before_v*` and `after_v*` are all set to
`NULL` an addition `mz_progressed` column is inserted before
`mz_state`.

Example:
```sql
> CREATE TABLE kv_store(key int, value int, UNIQUE(key))
> INSERT INTO kv_store VALUES (1, 2), (2, 4)
> SUBSCRIBE kv_store ENVELOPE DEBEZIUM (KEY (key))
mz_timestamp | mz_state        | key  | before_value | after_value
-------------|-----------------|------|--------------|--------------
100          | insert          | 1    | NULL         | 2
100          | insert          | 2    | NULL         | 4

...
-- at time 200, update key=1's value to 10
...

200          | upsert          | 1    | 2            | 10


...
-- at time 300, add a new row with key=3, value=6
...

300          | insert          | 3    | NULL         | 6

...
-- at time 400, delete all rows
...

400          | delete          | 1    | 10           | NULL
400          | delete          | 2    | 4            | NULL
400          | delete          | 3    | 6            | NULL

...
-- at time 500, introduce a key violation
...

500          | key violation   | 1    | NULL           | NULL

```

Today only one output modifier can be used at a time. `ENVELOPE
DEBEZIUM` and `ENVELOPE UPSERT` are conflicting directions. `WITHIN
TIMESTAMP ORDER BY` and `ENVELOPE DEBEZIUM` are hard to use together
since `DEBEZIUM` produces two copies of the value and it would be
difficult to distinguish between them. In theory, `WITHIN TIMESTAMP
ORDER BY` and `ENVELOPE UPSERT` could be used together if the client
wanted a specific sort order out of subscribe. There are few cases
where that's useful so I propose disallowing it until there is client
demand.

# Reference explanation
[reference-explanation]: #reference-explanation

For each of the three modes we're only changing the results for a
given timestamp. The sorting and changing the output format can be
done in `ActiveSubscribe::process_response` which is guaranteed to
have a complete `SubscribeResponse`'s batch of data. The returned
batches [don't
overlap](https://github.com/MaterializeInc/materialize/blob/e781222ea382aa6a7eee1b0e50e94218988f4cad/src/compute-client/src/protocol/response.rs#L106-L110)
so we can operate on one batch at a time.

`WITHIN TIMESTAMP ORDER BY`: in `process_response`, for each timestamp
we'll sort the rows by the given `ORDER BY`. This `ORDER BY` can be as
complicated as any `SELECT`. This adds adds some logic to compiling a
`SUBSCRIBE` plan but we can reuse most of the existing planning
machinery.

`ENVELOPE UPSERT` and `ENVELOPE DEBEZIUM`: similar to `WITHIN
TIMESTAMP ORDER BY` the transformation can be done locally in
`process_response`: the set of retractions and additions uniquely
determine the resulting upsert envelopes. The planning work is small:
finding the indexes of the key columns. The changes to describe are a
tad more interesting: we have to reorder columns and remove the
`mz_diff` column.

For `ENVELOPE UPSERT`, there are four cases for a given timestamp and key.
 - [(k, v, +1)] => (upsert, k, v)
 - [(k, v, -1)] => (delete, k, NULL)
 - [(k, v1, -1), (k, v2, +1)] => (upsert, k, v2)
 - everything else => (key violation, k, NULL)
 
"Everything else" includes multiple new values for a given key,
multiple retractions of given values (perhaps key violations that
happened in the past that we may not have known about), or a mix.

For `ENVELOPE DEBEZIUM`, we have the same four cases as `ENVELOPE
UPSERT`, just slightly different outputs:
 - [(k, v, +1)] => (insert, k, NULL, v)
 - [(k, v, -1)] => (delete, k, v, NULL)
 - [(k, v1, -1), (k, v2, +1)] => (upsert, k, v1, v2)
 - everything else => (key violation, k, NULL, NULL)

# Rollout
[rollout]: #rollout

We'll put these options into production behind a launchdarkly flag
that we can enable for clients if they're okay working without a
backwards compatibility guarantee. That'll also let us gather feedback
from clients using these features.

## Testing and observability
[testing-and-observability]: #testing-and-observability

New testdrive tests that exercise these new output types.

There isn't a performance concern because these output types only need
to sort the data for a given timestamp.

We'll add metrics to prometheus to see how often these options are
used in order to find clients that are using subscribe and get more
detail on their usecases.

## Lifecycle
[lifecycle]: #lifecycle

The options will start life behind individual feature flags:
 - `enable_within_timestamp_order_by_in_subscribe`
 - `enable_envelope_upsert_in_subscribe`
 - `enable_envelope_debezium_in_subscribe`

After gathering feedback we'll have to decide if feature is worth the
addition to the surface area of the language. Then we can promote it to
stable.

# Drawbacks
[drawbacks]: #drawbacks

We're adding surface area to MZ's SQL dialect. While adding these
output types under a feature flag will let us avoid backwards
compatibility guarantees, it's always hard to take features away.

# Conclusion and alternatives
[conclusion-and-alternatives]: #conclusion-and-alternatives

An alternative to this design is to tackle our client's underlying
problems more directly. The underlying subscribe protocol gives an
accurate representation of the current state of the query results. If
there's demand for specific views on this state we could provide those
directly with a client side library. For example, a common usecase I
can imagine would be maintaining the results of a query client side in
order to display them. Even with these output types it's not
straightforward to implement: the client has to write a little state
machine in order to tell when it has the complete snapshot data and
when it has a consistent view on the result set. A example
implementation:

```python
"""
Maintains a map from the first `num_primary_keys` columns to the rest of the row
Calls handleSnapshot with that map when there's a complete snapshot
Calls handleUpdated with that map every time we've gotten on update
`query` needs to ask for PROGRESS and SNAPSHOT
"""
def subscribe(query, num_primary_keys, handleSnapshot, handleUpdated):
    table = {}
    conn = psycopg.connect("user=...")
    with conn.cursor() as cur:
        first_message = True
        saw_complete_snapshot = False
        cur_batch_timestamp = None
        saw_data_updates_in_cur_batch = False
        for (progress, timestamp, diff, *data) in cur.stream(query):
            if timestamp != cur_batch_timestamp:
                # process the batch we just worked on unless it's the first message
                if first_message:
                    first_message = False
                elif saw_complete_snapshot:
                    if saw_data_updates_in_cur_batch:
                        handleUpdated(table)
                else:
                    saw_complete_snapshot = True
                    handleSnapshot(table)
                saw_data_updates_in_cur_batch = False
                cur_batch_timestamp = timestamp

            if not progress:
                saw_data_updates_in_cur_batch = True
                for _ in range(diff):
                    table[data[:num_primary_keys]] = data[num_primary_keys:]
                for _ in range(-diff):
                    if data[:num_primary_keys] in table and table[data[:num_primary_keys]] == data[num_primary_keys:]:
                        del table[data[:num_primary_keys]]
```

This design will only help with the bottom paragraph, when updating `table`.

A client library can do things that aren't possible using the pgwire
format, like call callbacks or maintain state. On the other hand it's
a cumbersome tool: we'd need to maintain different implementations for
different languages and it might be tempting to cheat and use features
that aren't exposed to the pgwire format in the future. It's not clear
to me if there's a small set of primitive operations that would cover
enough use cases to carry their weight.

The goal of this design is to enable clients to more easily use
`SUBSCRIBE`. It's a smaller step than whole client side libraries and
may help. If it doesn't succeed we can try something else.

# Unresolved questions
[unresolved-questions]: #unresolved-questions

None

# Future work
[future-work]: #future-work

The ultimate point of this work is making it easy for clients to use
`SUBSCRIBE` in their applications. This is a step is gathering enough
data to know what to do next.
