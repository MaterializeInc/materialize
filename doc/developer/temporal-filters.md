# Temporal Filters

Materialize is able to introduce and remove records as a function of timestamps presented as data in the records.
This is performed using idiomatic SQL, with some restrictions, but it can be hard to understand at first.

Consider the following SQL query:
```sql
SELECT *
FROM data
WHERE mz_logical_timestamp() BETWEEN data.valid_from AND data.valid_until;
```
The `WHERE` clause here relates two columns in the data to `mz_logical_timestamp()`, a function that returns the current logical timestamp.
For one-shot queries `mz_logical_timestamp()` is simply evaluated and replaced by that timestamp when the query executes.
In a *maintained* query, as for example when you call `CREATE MATERIALIZED VIEW` or `CREATE INDEX`, the value of the function changes every millisecond.
In particular, in a maintained query any row will not be present at timestamps before its `valid_from` column nor after `valid_until` column.

Materialize does not re-evaluate the predicate every millisecond for every row.
Understanding what Materialize does do may help you understand the behavior of and restrictions on temporal filters.

## Manipulating timestamped updates

Materialize records *updates* to collections of data.
Each updates has the form `(data, time, diff)`, and indicates that the occurence count of `data` changes at "logical timestamp" `time` by `diff`.
An update occurs at its logical timestamp and is then in effect indefinitely.
To undo an update one issues a new update with the same `data` at a future logical timestamp with a negated `diff`.

We can restrict the duration of an update, from `[time, ..)` to any non-empty interval `[from, until]`, by
1. advancing the update's timestamp to at least `from`, and
2. introducing a new negated update at timestamp `until`.
The first step ensures that the update does not happen until `from` and the second step ensures that the update is retracted at `until`.
Each of the two steps is optional, if either of the `from` or `until` bounds are unspecified.

This mechanism allows us to support `WHERE` clauses in which `mz_logical_timestamp()` is directly related to an expression not containing `mz_logical_timestamp()`, or conjunctions (`AND`) of these clauses.
The following query demonstrates several example uses:
```sql
SELECT *
FROM data
WHERE mz_logical_timestamp() BETWEEN data.valid_from AND data.valid_until
  AND mz_logical_timestamp() > 100
  AND 567 > mz_logical_timestamp();
```
The `BETWEEN` expression simplifies to two inequalities, and the other two inequalities are one-sided bounds.
The `mz_logical_timestamp()` call can appear on either side of the inequality, but it must occur by itself.
Any conjunction of these inequalities is valid, including just bounding `mz_logical_timestamp()` from above or below, rather than bounding it on both sides.

The inequality cannot be "not equal" (`!=` or `<>`) as this results in a "hole" rather than an upper and lower bound.

## Correctness

Using a temporal filter does not ensure that a record will be present at `valid_from`.
The update that introduces a record has its own logical timestamp, and a temporal filter can only advance that time.
To ensure precise validity consider using a CDC format that allows you to specify update timestamps explicitly.

## Performance

Materialize will re-evaluate downstream views at each timestamp presented to them.
These timestamps are denominated in milliseconds, but are often much more coarse-grained because of a source's timestamping policy.
For example, the default source timestamping frequency is once every second.
This means that downstream views will only see the collection change once per second, and e.g. each aggregate value will change at most once per second.
This reduces the computational burden over a system whose values changed every millisecond.

Temporal filters allow you to introduce updates with timestamps that do not otherwise exist in the input.
If your `valid_from` and `valid_until` columns are arbitrary milliseconds not aligned to seconds, you may prompt many additional recomputations and substantially impair Materialize's performance.
Please experiment with the performance, and consider coarsening your validity bounds if performance requires or if your query permits.
For example, you could round your temporal bounds to multiples of your source timestamping frequency.

Temporal filters allow you to introduce updates arbitrarily far in the future.
Materialize is optimized to maintain collections and queries "now", and it is not optimized as a data store for far-future updates.
Until the timestamps in your temporal bounds come to pass, each record will impose an ongoing cost in memory and compute to Materialize.
If you find you need to use far-future times and the performance is less acceptable than with near-term times, please get in touch.
