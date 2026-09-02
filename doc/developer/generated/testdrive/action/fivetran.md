---
source: src/testdrive/src/action/fivetran.rs
revision: 849327076c
---

# testdrive::action::fivetran

Testdrive action for exercising a running Fivetran Destination service over gRPC.

`run_destination_command` accepts a `BuiltinCommand` with an `action` argument (`describe` or `write_batch`) and an optional set of connection key-value arguments. It builds a connection configuration by merging any command arguments with defaults derived from the current Materialize SQL address, then reads two JSON objects from the command body: the request payload and the expected response. It splices the connection configuration into the request under the `"configuration"` key, dispatches the appropriate gRPC call (`DescribeTable` or `WriteBatch`) to the Fivetran destination at `state.fivetran_destination_url`, and fails the test if the actual response does not match the expected value.

The module embeds generated protobuf bindings for `fivetran_sdk.v2` and `google.protobuf` (well-known types) via `include!` macros pointing at `OUT_DIR`.
