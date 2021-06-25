---
title: "Temporal Filters"
description: "Perform time-windowed computation over temporal data."
weight: 10
menu:
  main:
    parent: guides
---

You can use temporal filters to perform time-windowed computations over temporal data.

--Define temporal data

## Temporal Data
Temporal databases are a pretty neat thing, and we aren’t going to go deep on that today. Instead, let’s just start with a relation that has a pretty simple schema, and which includes some times.

```sql
-- Making a new data source.
CREATE TABLE events (
    content text,
    insert_ts numeric,
    delete_ts numeric
);
```

We have some content, clearly you could fluff that up to something more interesting, and two additional columns. The `insert_ts` and `delete_ts` columns will play the role of timestamps, indicating when an event should first appear and when it should be removed.

Of course, this is all just data right now. These are fields, and you could put whatever you want in them. They don’t have any specific meaning yet. You could make `delete_ts` be smaller than `insert_ts`, if you are that sort of person.

## Time-Windowed Queries

The question is now what do you do with these data?

In Materialize, you can ask questions that grab the “logical time” for your query (roughly: the wall clock when you run the query) and then use that in your query’s logic. For example, this query counts up content based on events that start by and end after the current logical timestamp.

```
-- Reduce down to counts within a time interval.
SELECT content, count(*)
FROM events
WHERE mz_logical_timestamp() >= insert_ts
  AND mz_logical_timestamp()  < delete_ts
GROUP BY content;
```

This query will change its results over time. Not just because you might add to and remove from events, but because `mz_logical_timestamp()` advances as you stare at your screen. Each time you issue the query you may get a different result.

This looks like a great query! What’s not to like?

The main issue really is that it is just a query. You can ask this question over and over, but you can also ask the same thing with an arbitrary timestamp in place of `mz_logical_timestamp()`. To support that, Materialize has to keep the entire collection of data around. Your events table will grow and grow and grow, and the time to answer the query will grow as well.

## Time-Windowed Computation
Materialize specializes at maintaining computations like the above, both because that can make it faster to get your answers out, but also because by specifying what you actually need Materialize can run much more lean. We’ll see that now with the query above!

Until recently, if you tried to create a materialized view of the query above, Materialize would tell you to take a hike. The subject of this post is that you can now do it. Moreover, comparing parts of your data to `mz_logical_timestamp()` in views (rather than just in queries) introduces powerful new idioms, ones that we’ll explore in this post.

```sql
-- Maintained collection of only valid results.
CREATE MATERIALIZED VIEW valid_events AS
SELECT content, count(*)
FROM events
WHERE mz_logical_timestamp() >= insert_ts
  AND mz_logical_timestamp()  < delete_ts
GROUP BY content;
```
What’s all this then?

Presumably `valid_events` has the property that if you `SELECT` from it you should see the same results as for the time-windowed `SELECT` in the previous section. That is 100% true.

What is also true is that `valid_events` has enough information from you, in the form of the query itself, to maintain only enough historical detail to answer these `SELECT` queries from now onward. Once Materialize’s `mz_logical_timestamp()` passes a record’s` delete_ts` it cannot be seen (at least not through this view), and Materialize can dispose of the event. The in-memory footprint of `valid_events` stays bounded by the number of records in the system that could still satisfy this constraint (those records that are currently valid, or who may yet become valid in the future).

While you add to events, Materialize collects up the events that are no longer visible, automatically. Of course, you can also change the records in events, in case you want to remove some events early, or draw out the` delete_ts` of any record, or replace one event with its next stage (and new `insert_ts` and `delete_ts`). If you happen to adjust any fields that interact with `mz_logical_timestamp()` Materialize will update the views appropriately.

## A Brief Example

Let’s do some testing with our table and maintained view. Tables have the nice property that we can interactively update them from within Materialize, rather than spinning up a Kafka cluster.

Let’s start with something simple: we’ll just look at the records currently present in our `valid_events view`. Let’s define a different view to do that, though, without the aggregation so you can see the raw data:

```sql
-- Maintained collection of only valid results.
CREATE MATERIALIZED VIEW valid AS
SELECT content, insert_ts, delete_ts
FROM events
WHERE mz_logical_timestamp() >= insert_ts
  AND mz_logical_timestamp()  < delete_ts
```

We’ll print out the things in our view, along with the current logical timestamp. It is initially empty, because we haven’t put any data in. But, these are the columns we’ll be looking at.

```sql
materialize=> SELECT *, mz_logical_timestamp() FROM valid;
 content | insert_ts | delete_ts | mz_logical_timestamp
---------+-----------+-----------+----------------------
(0 rows)
```

Now let’s put some data in there. I’m going to just take advantage of the fact that `INSERT` statements can also use `mz_logical_timestamp()` to populate the data with some records that we will make last five seconds.

```sql
materialize=> insert into events VALUES (
    'hello',
    mz_logical_timestamp(),
    mz_logical_timestamp() + 5000
);
materialize=> insert into events VALUES (
    'hello',
    mz_logical_timestamp(),
    mz_logical_timestamp() + 5000
);
materialize=> insert into events VALUES (
    'hello',
    mz_logical_timestamp(),
    mz_logical_timestamp() + 5000
);
```

Each of these were executed by me, a human, and so almost certainly got different `insert_ts` and `delete_ts` timestamps. We’ll see them in just a moment!

Next, I typed incredibly fast to see the output for the query; what was previously empty just up above:

```sql
materialize=> SELECT *, mz_logical_timestamp() FROM valid;
 content |   insert_ts   |   delete_ts   | mz_logical_timestamp
---------+---------------+---------------+----------------------
 hello   | 1613084609890 | 1613084614890 |        1613084613168
 hello   | 1613084611459 | 1613084616459 |        1613084613168
 hello   | 1613084610799 | 1613084615799 |        1613084613168
(3 rows)
```

We can see that the insert_ts and delete_ts values are indeed 5000 apart, and for each of the outputs the mz_logical_timestamp lies between the two. What happens if we type the query again, very quickly?

```sql
materialize=> SELECT *, mz_logical_timestamp() FROM valid;
 content |   insert_ts   |   delete_ts   | mz_logical_timestamp
---------+---------------+---------------+----------------------
 hello   | 1613084609890 | 1613084614890 |        1613084613988
 hello   | 1613084611459 | 1613084616459 |        1613084613988
 hello   | 1613084610799 | 1613084615799 |        1613084613988
(3 rows)
```

The `mz_logical_timestamp` values have increased. We still see all of the record, as the timestamp hasn’t increased enough to fall outside the five second bound yet. We type again ..

```sql
materialize=> SELECT *, mz_logical_timestamp() FROM valid;
 content |   insert_ts   |   delete_ts   | mz_logical_timestamp
---------+---------------+---------------+----------------------
 hello   | 1613084609890 | 1613084614890 |        1613084614843
 hello   | 1613084611459 | 1613084616459 |        1613084614843
 hello   | 1613084610799 | 1613084615799 |        1613084614843
(3 rows)
```

.. and the timestamp increases again ..

```sql
materialize=> SELECT *, mz_logical_timestamp() FROM valid;
 content |   insert_ts   |   delete_ts   | mz_logical_timestamp
---------+---------------+---------------+----------------------
 hello   | 1613084611459 | 1613084616459 |        1613084615628
 hello   | 1613084610799 | 1613084615799 |        1613084615628
(2 rows)
```

.. and we lost one! Now that `mz_logical_timestamp()` has reached 1613084614890 that record no longer satisfies the predicate, and is no longer present in the view.

```sql
materialize=> SELECT *, mz_logical_timestamp() FROM valid;
 content |   insert_ts   |   delete_ts   | mz_logical_timestamp
---------+---------------+---------------+----------------------
 hello   | 1613084611459 | 1613084616459 |        1613084616392
(1 row)
```

One more has dropped out.

```sql
materialize=> SELECT *, mz_logical_timestamp() FROM valid;
 content | insert_ts | delete_ts | mz_logical_timestamp
---------+-----------+-----------+----------------------
(0 rows)
```

Ah, they are all gone. My fingers can rest now.

Although this looks rather similar to re-typing the `SELECT` query that explicitly filters against `mz_logical_timestamp()`, the difference here is that everything is dataflow with updates flowing through it. If we were to `TAIL` the view, we would see exactly the moments at which the collection changes, without polling the system repeatedly.

And of course, we can handle a substantially higher volume of updates than if we were continually re-scanning the entire collection.

## Windows: Sliding and Tumbling

The pattern we saw above was actually very powerful: records could state both their insertion and deletion times. If a record wants to be around for 10s it can do that, if it wants to stay for a year or forever, it could do that too (note: please read further before choosing to do that).

But, let’s check out some other idioms.

Sliding windows are fixed-size time intervals that you drag over your temporal data, and whose query results should be the as if run on the subset of records in the interval. These windows can be great if you want to maintain an always-up-to-date view of your most recent data.

The example we had above where all records were valid for five seconds was a sliding window, though just because we happened to enter the data the right way. We can just change the query to ensure that we get the right view.

-- Slide a 5 second window over temporal data.
CREATE MATERIALIZED VIEW valid_events AS
SELECT content, count(*)
FROM events
WHERE mz_logical_timestamp() >= insert_ts
  AND mz_logical_timestamp()  < insert_ts + 5000
GROUP BY content;
Here we’ve changed the query ever so slightly, to ignore the records delete_ts field and just impose an upper bound of five seconds after the insertion. This ensures that even silly records will get cleaned up soon enough. You could add back in the delete_ts bound if you wanted folks to be able to drop out of windows too, of course.

Importantly, the insert_ts field can be whatever you want. It is in milliseconds, and your output sliding window will update as many as 1,000 times per second. This is called a “continual slide” window. You aren’t obliged to have the 5 second window hop only on second boundaries, or anything like that.

I mean, you could, if that is what you want; you just tweak the query:

-- Slide a 5 second window over temporal data, second-by-second.
CREATE MATERIALIZED VIEW valid_events AS
SELECT content, count(*)
FROM events
WHERE mz_logical_timestamp() >= 1000 * (insert_ts / 1000)
  AND mz_logical_timestamp()  < 1000 * (insert_ts / 1000) + 5000
GROUP BY content;
The granularity and width of the window is up to you to control, with straight-forward SQL.

Tumbling windows (sometimes: “hopping”) are just those coarse-grained sliding windows that slide one unit at a time. Each record contributes to only one window.

-- Tumble a 1 second window over temporal data, second-by-second.
CREATE MATERIALIZED VIEW valid_events AS
SELECT content, count(*)
FROM events
WHERE mz_logical_timestamp() >= 1000 * (insert_ts / 1000)
  AND mz_logical_timestamp()  < 1000 * (insert_ts / 1000) + 1000
GROUP BY content;
I think these windows have a special name because they are much easier to implement for non-streaming systems. They are also useful if you want to see aggregate values that can just be added up to get totals for a larger time interval. If you wanted per-minute totals you could add up 60 of the numbers above, and from them hourly totals, etc.

## Going Beyond Count

Perhaps this is obvious, but you can do more than just count(*) things. The valid view we produced up above, containing all currently valid events, is just like any other materialized view in Materialize, and you can use it as you like. Join it with other temporal relations, put it in a correlated subquery, feed it in to an exotic jsonb aggregation.

This feature is the main difference between what is going on in Materialize, and in time-series databases (TSDBs). TSDBs are good at storing historical measurements and serving them up when you ask, but they aren’t generally as good at maintaining non-trivial computation over arbitrarily changing data. They can usually handle counts and sums pretty well, but if you want to maintain complex views over your changing, temporal data I recommend trying out Materialize.

Let’s do a quick example with some non-trivial joins.

The TPC-H benchmark is a collection of 22 decision support queries. To pick one mostly at random (ed: lies), query 3 looks like

SELECT
    o_orderkey,
    o_orderdate,
    o_shippriority,
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < DATE '1995-03-15'
    AND l_shipdate > DATE '1995-03-15'
GROUP BY
    o_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10;
This query determines the top ten unshipped orders by value as of some date (here: '1995-03-15'). Wouldn’t it be neat to instead monitor it for all dates, continually, as it happens?

All we need to do is change those two lines with the '1995-03-15' in them.

...
    AND o_orderdate < mz_logical_timestamp()
    AND l_shipdate > mz_logical_timestamp()
...
That was an easy change to type. Let’s discuss what these new bounds do.

The first changed constraint restricts our attention to orders placed before “now”, which has the effect of keeping orders out of the query until we’ve reached their o_orderdate column. In a real setting, this is probably a bit of a no-op, in that the record probably lands in our input stream around the order date anyhow, and doesn’t need to be suppressed before then.

The second changed constraint restricts our attention to lineitem records that have not shipped by “now”. This has the effect of deleting the record once we reach l_shipdate, effectively garbage collecting that relation for us, which is especially nice as lineitem is the largest of the input relations (it is a “fact table”), and which churns the most.

Expressed this way, with temporal filters, the memory footprint of the view will be proportional to the sizes of orders and customer, plus as much of lineitem is present but has not yet shipped. If we wanted to tighten our belt even more, we could add a further constraint that we aren’t looking at orders that are too old

    AND o_orderdate + '90 days' > mz_logical_timestamp()
This will collect up the orders relation in addition to lineitem, and also prevent us from always seeing that one order from two years back that never shipped.

Conclusions
Temporal filters are pretty neat stuff. I hope you are half as excited as I am.

There are some limitations. I should have mentioned this earlier.

You can only use mz_logical_timestamp() in WHERE clauses, where it must be directly compared to expressions not containing mz_logical_timestamp(), or in a conjunction (AND) with other clauses like that. You aren’t allowed to use != at the moment, but clever folks could figure out how to fake that out. For the reasoning on all this, check out the implementation discussion next!

Limitations notwithstanding, I’m personally very excited about these temporal filters. They open up the door to functionality and behaviors that streaming systems only provide through special language extensions. But, all you really need is SQL, and the ability to refer to time, to make your data run.

Get access to Materialize here. Temporal filters aren’t released yet, so to try it out you’ll need to either build from source or pull down the right docker image, and use the --experimental flag. It should be available soon in an upcoming release. In the meantime, take a swing by the Materialize blog for more cutting-edge content, and join the community Slack through the bright banner at the top of the Materialize homepage.

## Some of you are surely here to hear how the magic works.

The magic lives in filter.rs, which is the Rust source for our filter operator. Normally, the filter logic is very simple, and evaluates predicates against records and drops those records that do not pass the predicate. That code was sufficiently simple that it did not previously merit its own file (it was 10 lines of code, roughly).

However, this all changed with temporal filters, which need to do something more clever than just drop or retain things. Let’s talk through what they need to do first, before we see how they go about doing it.

In differential dataflow, which lies in wait underneath Materialize, dataflow operators consume and produce updates: triples of (data, time, diff). The data is the data payload: the values in the columns of your individual records. The time is the logical timestamp at which the change should take effect. The diff is .. a signed integer let’s say, that says how the occurence count of data should change: postitive numbers indicate additions, negative numbers indicate deletions. Each stream of updates describes a continually changing collection, whose contents can be determined at any time by accumulating up the appropriate updates.

The traditional (non-temporal) filter responds to (data, time, diff) triples by applying a predicate to data, and either dropping or retaining the triple based on what it sees. However, if we did that with a temporal predicate only at the moment we received the update, I guess using the current mz_logical_timestamp(), we wouldn’t do the right thing at all. We might drop the record as being too early yet, oblivious to the fact that the record should re-appear in the future. Similarly, if the record should be removed in the future, evaluating the predicate now doesn’t have the right effect.

The temporal filter is somewhat less traditional than its non-temporal counterpart. Rather than drop or retain records right now, it will schedule the insertion and deletion of records in the future.

The temporal filter operator looks for predicates of the form

mz_logical_timestamp() CMP_OP EXPRESSION
where CMP_OP is a comparison operation other than != (i.e. the operators =, <, <=, >, >= and things like BETWEEN that reduce to them) and EXPRESSION is an expression that does not contain mz_logical_timestamp(). Roughly, the expression is a function of data, and once we evaluate it we get a bound on mz_logical_timestamp(). If we have several comparisons, we end up with bounds, maybe lower and maybe upper, which describe an interval of time.

An update (data, time, diff) normally takes effect at time and is in then effect indefinitely. However, we can narrow its range of time to [lower, upper) by transforming the input update into two output updates:

(data, max(time, lower), diff)
(data, upper, -diff)
This change delays the insertion of data until at least lower, and schedules its deletion at upper.

There are a variety of corner cases to double check, mostly around what to do if a bound is absent, or if they cross (you can write it; we need to make sure it doesn’t break). You’ll want to double check that the above makes sense when diff is negative (a deletion undoes the window its insertion would have introduced). We also need to update our query optimizer as filters can now do slightly weirder things than they could before, and it is less clear that you should use these filters e.g. to drive equi-join planning.

But actually, the above is basically the implementation. The whole file comes in at around 300 lines, and that’s with comments and a copyright header.

There are surely a lot more lines of code to write in response to all the issues you are about to file, but I’m looking forward to that!
