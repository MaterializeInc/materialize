# Introduction

The purpose of this framework is to test the product limits in various dimensions (e.g. number of kafka sources,
number of tables in a join).

Some of the limits are product-wide, most pertain to the various aspects of individual queries that
pertain to the optimizer, e.g. "number of conditions over the same column in a WHERE clause".

While the framework does not explicitly test for performance and can not intelligently flag regressions,
in practice it is hoped that regressions will be caught by one of the following mechanisms:
- a fixpoint panic if the optimizer is unable to transform a complex query to a stable tree
- a stack overflow if a recursive algorithm is used anywhere during query processing
- a testdrive of a global CI timeout if an O^M(N) algorithm is used. As we are using N = 1000 in most tests,
  an algorithm with M ~ 3 should make things time out and fail in CI
- a panic on massive memory leaks, as the Mz container runs with --memory 2G (when supported by docker-compose)

# Running

```bash
./mzcompose down -v

./mzcompose run default
```

# Adding new content

subclass the `Generator` class and define a `body()` class method that uses the provided iterators to generate the
desired query in testdrive format and `print()` it.
