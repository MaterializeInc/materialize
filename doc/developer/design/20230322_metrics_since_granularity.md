- Feature name: Retained metrics "since" granularity
- Associated: https://github.com/MaterializeInc/materialize/issues/17741

# Summary
[summary]: #summary

Currently, all objects maintained by Materialize have a read policy (i.e., retention window) that lags the write frontier by between one and two times some specified value. For example, if that value is one second, the read frontier will lag the write frontier by between 1000 and 2000 milliseconds; concretely, the read frontier will be the largest milliseconds-since-epoch value that is a multiple of 1000 and at least 1000 less than the write frontier. This design proposes to separate the concepts of "minimum lag" and "since granularity" such that those two numbers (both "1000" in the example) can be varied independently.

# Motivation
[motivation]: #motivation

The reason for the current design is that if we always lagged by exactly the value of the retention window, then we would create large amounts of traffic for frontier update messages between `environmentd` and `clusterd`, and between Persist and cockroach, since every time the write frontier were updated, we would update the corresponding read frontier. Thus, we limit these messages to once per second.

However, as retention window increases, the importance of limiting this traffic becomes relatively less important, and the size of wasted space due to unnecessary retention becomes relatively more important. For example, if the retention window is set to 30 days, we will be storing anywhere between 30 and 60 days of data for a given relation; this extra 2x slop is unnecessarily wasteful. We would like to increase the granularity of updates, for example to 1 day, which would make it so we store between 30 and 31 days of data for a given relation while also not appreciably increasing `AllowCompaction` traffic (as we will still only send one such message per day).

# Explanation
[explanation]: #explanation

This design does not propose making this value user-configurable; we only want to tweak it for the "retained metrics relations", whose retention is currently set via LaunchDarkly to 30 days. We will make the following changes:

1. Add a new LaunchDarkly parameter for `retained_metrics_since_granularity`, which will be set to 1 day by default.
2. Add a "granularity" parameter to [`ReadPolicy::lag_writes_by`](https://github.com/MaterializeInc/materialize/blob/eda55b5c68/src/storage-client/src/controller.rs#L516-L530).
3. For all objects with `is_retained_metrics_relation` set to true, use that LaunchDarkly parameter as the value of the parameter discussed in point 2.
4. For all other objects, continue using 1 second as the default.

## Testing and observability
[testing-and-observability]: #testing-and-observability

We can test this by parsing `EXPLAIN TIMESTAMP` output in a testdrive test (assuming we have the ability to mock LD values there, which I think we do).

## Lifecycle
[lifecycle]: #lifecycle

No known risks

# Drawbacks
[drawbacks]: #drawbacks

Slightly increases system complexity

# Future work
[future-work]: #future-work

We still need to complete the other tasks in https://github.com/MaterializeInc/materialize/issues/18347 .
