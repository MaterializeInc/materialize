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
