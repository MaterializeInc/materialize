# mz-subscribe

A correct-by-construction Rust client for consuming Materialize
[`SUBSCRIBE`](https://materialize.com/docs/sql/subscribe/).

```rust
use mz_subscribe::{Subscribe, SubscribeClient};

let client = SubscribeClient::connect("postgres://materialize@localhost:6875").await?;
let mut stream = client
    .subscribe(Subscribe::object("winning_bids").envelope_upsert(["id"]))
    .await?;

while let Some(batch) = stream.next().await? {
    for change in &batch.updates {
        // apply `change` to your sink
    }
    // Persist `batch.resume_token` atomically with the applied changes for
    // exactly-once state; hand it back to `client.resume` on restart.
}
```

* The unit of consumption is a `ConsistentBatch`, never a bare row.
* Resuming takes an opaque `ResumeToken`; the `AS OF frontier - 1` arithmetic
  lives inside it.
* Failures surface as a small, typed `SubscribeError`.
* For a lower-level stream of decoded changes and progress markers, use
  `subscribe_raw`. The batcher is built on it.

## Cohorts

To stay consistent across several views at once, a `Cohort` releases every
member only up to their shared minimum frontier, so each moment is a genuine
cross-view snapshot.

```rust
use mz_subscribe::{Cohort, Subscribe};

let mut cohort = Cohort::connect(
    "postgres://materialize@localhost:6875",
    vec![
        ("orders".to_string(), Subscribe::object("orders")),
        ("inventory".to_string(), Subscribe::object("inventory")),
    ],
)
.await?;

while let Some(moment) = cohort.next().await? {
    for view in &moment.views {
        // view.name, view.updates: this view's changes at the joint frontier
    }
    // moment.resume_token checkpoints the whole cohort at once.
}
```

See the [top-level README](../README.md) for the protocol this encodes and the
guarantees it provides.

## Development

```
cargo test
cargo clippy --all-targets -- -D warnings
```

Set `MZ_SUBSCRIBE_TEST_DSN` to run the ignored live smoke test against a
Materialize instance.
