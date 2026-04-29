# Bounded staleness isolation — happy-path coverage.

> CREATE TABLE bs_t (a int);
> INSERT INTO bs_t VALUES (1), (2), (3);

> SET transaction_isolation = 'bounded staleness 5s'

> SELECT count(*) FROM bs_t;
3

> SELECT sum(a) FROM bs_t;
6

# Round-trip via SHOW.
> SHOW transaction_isolation
"bounded staleness 5s"

> RESET transaction_isolation

> DROP TABLE bs_t

# --- error path: bounded staleness exceeded -----------------------------------
#
# Under a tiny bound, freshly-created collections may not have advanced their
# upper frontier within the staleness window, exercising the
# BoundedStalenessExceeded error path. The test depends on the timing of
# frontier advancement; if it becomes flaky in CI, replace with a deterministic
# stall primitive when one is available.

> CREATE TABLE bs_slow (a int);

> SET transaction_isolation = 'bounded staleness 1ms'

! SELECT count(*) FROM bs_slow
contains: cannot serve query under bounded staleness

> RESET transaction_isolation

> DROP TABLE bs_slow
