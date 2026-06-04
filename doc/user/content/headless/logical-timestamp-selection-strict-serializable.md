---
headless: true
---
When using the [strict
serializable](/reference/isolation-level#strict-serializable) isolation level,
Materialize attempts to keep the logical timestamp reasonably close to wall
clock time. In most cases, the logical timestamp of a query will be within a few
seconds of the wall clock time. For example, when executing a strict
serializable query at a wall clock time of 9pm, Materialize will choose a
logical timestamp within a few seconds of 9pm, even if data for 8:30–9pm has not
yet arrived and the query will need to block until the data for 9pm arrives. In
this scenario, both `now()` and `mz_now()` would return 9pm.
