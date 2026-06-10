---
source: src/persist-cli/src/maelstrom/node.rs
revision: 82d92a7fad
---

# persistcli::maelstrom::node

Implements the Maelstrom RPC driver: `run` reads newline-delimited JSON messages from stdin and dispatches them through `Node<S>` to a user-supplied `Service` implementation.
`Node` handles the init handshake, routes service responses to waiting callbacks via `Core`, and spawns async tasks for each incoming workload request.
`Handle` is the public API given to `Service` impls: it provides `send_res` for workload responses, `send_service_req` for calls to Maelstrom services (lin-kv), and helpers like `lin_kv_read`, `lin_kv_write`, `lin_kv_compare_and_set`, and `maybe_init_shard_id`.
The `Service` trait defines the interface that `txn_list_append_single` and `txn_list_append_multi` implement.
