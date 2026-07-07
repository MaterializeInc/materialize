# materialize-subscribe

A correct-by-construction Python client for consuming Materialize
[`SUBSCRIBE`](https://materialize.com/docs/sql/subscribe/).

```python
from materialize_subscribe import Subscribe, SubscribeClient

with SubscribeClient.connect("postgres://materialize@localhost:6875") as client:
    stream = client.subscribe(
        Subscribe.object("winning_bids").envelope_upsert(["id"])
    )
    for batch in stream:
        for change in batch.updates:
            ...  # apply `change` to your sink
        # Persist `batch.resume_token.encode()` atomically with the applied
        # changes for exactly-once state; hand it back to `client.resume`.
```

* The unit of consumption is a `ConsistentBatch`, never a bare row.
* Resuming takes an opaque `ResumeToken`; the `AS OF frontier - 1` arithmetic
  lives inside it.
* Failures surface as a small, typed `SubscribeError` hierarchy.

The protocol core has no runtime dependencies. Install the `client` extra to
connect:

```
pip install 'materialize-subscribe[client]'
```

See the [top-level README](../README.md) for the protocol this encodes and the
guarantees it provides.

## Development

```
pip install -e '.[dev,client]'
pytest
mypy materialize_subscribe
ruff check .
```
