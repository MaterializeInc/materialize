# Timelines

## Summary

Some kinds of sources in Materialized have different meanings for the timestamps they attach to data.
If a user asks Materialized to join sources with different meanings of what a timestamp is, Materialized should tell the user that the timestamps can't be used together.
Tracking timelines (the meaning of a timestamp) provides a way for Materialized to produce errors when attempting to use data with different meanings of timestamps.

## Goals

1. Prevent users from doing things that are silently meaningless.
Joining timestamps with different meaning produces results that don't mean anything correct, and we should inform users of that with an error.
2. Prevent user queries blocking forever.
In addition to the above point, a query of that kind could potentially block forever.
This is a bad experience for users and we should prevent it from happening.
3. Unblock work on linearizability.
We would like to be linearizable in the future, and our current best understanding of how to do that is to have one timestamp per timeline that always increases when serving reads.

## Non-Goals

The API described here is easily misusable, because it is opt-in.
It is not a goal to make this misusable because the correct pieces are not yet in place.
We would like to be able to perform transactional catalog changes along with shipping new dataflows where we could detect timeline misuse, but there's some complicated work to be done there.
We can instead ship a solution that works in a much simpler way, with the potential for misuse (or rather, lack of use).

## Description

From dataflow's perspective, a timestamp is an arbitrary number, and dataflow will do whatever it is instructed when asked to join indexes at a specific timestamp number.
Most sources use something around wall-clock now when generating that number, so asking about the state of various indexes at time X will incur at most a very short (< 1 second) wait for the data to be ready.

A problem occurs when there is a source that generates timestamp numbers that may not be related to wall-clock now.
The two in our system are Debezium consistency topics and CDCv2.
Debezium consistency topics start their "timestamp" number at 1 and increment it for each transaction.
If these sources are joined with wall-clock now sources, the dataflow layer will happily wait for the timestamps to be equal.
For the Debezium sources, this would require waiting until there were as many transactions as milliseconds since the Unix epoch, which essentially will never happen.
We should prevent this poor UX from happening to users.

In addition to an infinite delay, there are cases where, even if the numbers were the same, they are meaningless when related to each other.
For example, two separate Debezium consistency topics.
Although they both start counting at 1 and thus have relatively close numbers, there's no meaning in the transaction counter, and joining data on that will not produce results that are meaningful.
We should inform users when they attempt to join data from differing timelines that they have not asked us a useful question.

The third concern is linearizability.
We would like to provide that property when users issue SELECT queries.
If we can distinguish the meaning of timestamps across data, we can provide linearizability across sources correctly.
(This will be described with more detail in a later doc.)

A solution to this problem is to inform the system what the timestamp number means for all data, and allow the system to reject queries that request data from multiple meanings.
We introduce a thing called a timeline which is the meaning of the timestamp number.
At the points in the system at which it is possible to join different sources together, we will return an error to the user if there is more than one timeline present.
There are two places where this can occur: `CREATE VIEW` and `SELECT`.
These have been taught to check that their expression depends at most one timeline.
Other operations that can create dataflow indexes (TAIL, CREATE SOURCE, CREATE SINK, CREATE INDEX) operate on existing views, and are thus unable to create dataflows from multiple timelines.

This is implemented by adding a `Timeline` enum, and requiring sources and tables to provide their `Timeline` to the Coordinator.
When a VIEW or SELECT is planned, its MirRelationExpr is crawled to discover all timelines.

Variants of `Timeline`:

- `EpochMilliseconds` for anything producing timestamps near wall-clock now.
- `Counter(struct)` for Debezium consistency topics, with a struct to track broker/topic to tell apart different sources.
- `External(String)` for CDCv2 sources with the source's name attached, making a CDCv2 source only joinable with itself.
- `User(String)` for user-defined timelines by a name chosen by the user.

All sources can specify `WITH (timeline='some_name')` to choose the `User` variant.
This will group any set of sources together if users need to join CDCv2 and realtime sources, or multiple CDCv2 sources.

By default, a CDCv2 source is joinable only with itself.
We do not allow them to join with even other CDCv2 sources because of side effects in linearizability and time domains, which might like to advance to the most recent time.
CDCv2 sources have a special `WITH (epoch_ms_timeline=true)` syntax to put that source into the EpochMilliseconds timeline.
Since we don't verify that the timestamps are indeed milliseconds from the epoch in this case, features that use this timeline (linearizability or transaction time domains, for example) will need to protect against data problems (for example, a CDCv2 source that is in the wrong units or has timezone errors, or is otherwise ahead or behind by many minutes).

## Alternatives

One alternative is to change the timestamp type to contain this enum.
If that's possible we could then error if there is more than one variant present, or we could maybe select from multiple timelines, each at their own value, and join data that way.
Changing the timestamp type is probably a large, difficult change.
We could also do another thing where the timestamp type is a (system time, event time) tuple, allowing each source to generate timestamps however it wants (event time), but using the system time when joining sources.

A second is to reclock all incoming data to EpochMilliseconds which would make all sources joinable to another based on when the system ingested them.
This would require us to keep a mapping back to the original timestamp for some data, since that might be meaningful to a user in the case of CDCv2.
This is also a very large change with lots of user-facing impact.

A third implementation is to teach either the dataflow builder or dataflow shipper to be able to error.
In the implementation above, we hand picked VIEW and SELECT to check that their dataflows are ok.
If we change something in the future and provide another way to create a potentially multi-timeline index, we might forget to check.
The above API is easily misusable, whereas teaching the dataflow bulider/shipper would be a global API that can't be misused.
Attempts were made to do this, but dataflow building and shipping interacts closely with the catalog, which doesn't support interactive (to the Coordinator, not the user) transactions.
Thus we didn't have a correct way to produce an error and rollback catalog changes.
We would like interactive catalog transactions in the future, so this may get done.
The attempts here illustrated that it was more difficult than expected, and so the work was tabled for a simpler solution to the timeline problem.

## Open questions

How safe is it to specify that a CDCv2 source is EpochMilliseconds?
What if a user does that, but their CDCv2 source is using seconds or microseconds instead of the expected milliseconds?
Or their timestamps are ahead or behind by minutes or hours?
This would cause the same problems that this proposal is trying to solve.
The simple solution there is to remove the view and fix the data.
A deeper difficulty occurs when we attempt linearizability, which would like these numbers to always go forward, and we don't want to block the entire system because some CDCv2 source had a very high timestamp that forces all other reads to wait forever.
This can maybe be solved by waiting to persist the linearizability tracker until a query has returned, but that could still have problems if that source wasn't joined to an actual EpochMilliseconds source.
