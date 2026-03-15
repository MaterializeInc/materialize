# actor — Log+S3 acceptor and learner (reference implementation)

An alternative implementation of the shared log consensus service that writes
log batches directly to object storage (S3 Express One Zone) and maintains its
own snapshot/recovery logic.

**Status: incomplete.** This implementation predates the persist-backed version
and is not used in production. It is kept as a reference for two reasons:

1. **Comparison.** Having a second implementation of the `Acceptor` and `Learner`
   traits helps validate the trait design and catch places where the abstraction
   is leaking implementation details from one backend.

2. **Benchmarking.** The specsheet workload simulator can run both backends,
   making it easy to compare latency and throughput characteristics.

## Modules

- **`acceptor.rs`** — Group commit actor: batches proposals on a timer, flushes
  to object storage, returns receipts. Includes fencing detection (competing
  writer on the same batch slot).
- **`learner.rs`** — State machine actor: tails the log (via push channel +
  fallback reads), evaluates CAS, materializes state, serves reads. Linearizes
  reads via the acceptor's `LastCommitted` shared atomic.
- **`metrics.rs`** — Prometheus metrics for both actors.
- **`storage.rs`** — `Storage` trait for log batch + snapshot I/O, with S3,
  simulated, noop, and latency-injection backends.
