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

See the [top-level README](../README.md) for the protocol this encodes and the
guarantees it provides.

## Development

```
cargo test
cargo clippy --all-targets -- -D warnings
```

Set `MZ_SUBSCRIBE_TEST_DSN` to run the ignored live smoke test against a
Materialize instance.
