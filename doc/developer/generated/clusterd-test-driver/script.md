---
source: src/clusterd-test-driver/src/script.rs
revision: 141cb2a0a5
---

# mz-clusterd-test-driver::script

AST and parser for the text command script format.

`Command` is an enum covering all supported script directives: `CreateInstance`, `UpdateConfiguration`, `InitializationComplete`, `DefineSchema`, `WriteRowsSingleTs`, `WriteRowsSpread`, `Define`, `DefineIndex`, `Peek`, `AwaitFrontier`, `AwaitSubscribe`, `Reconnect`, and others. Each variant carries its parsed parameters.

`CreateInstance` carries an `initial_config: Vec<ConfigSetting>` field (the create-time dyncfg snapshot the controller would supply), parsed from `name type value` body rows identical in format to those accepted by `update-configuration`. The snapshot is applied to the replica's worker config before create-time setup, so a scenario can assert that create-time work observes synced values rather than dyncfg defaults.

`ImportSpec`, `BuildSpec`, `ExportSpec`, `ColumnSpec`, `ConfigSetting` are supporting structs for the structured command bodies (MIR `define` sub-commands, schema definitions, dyncfg key-value pairs).

Object IDs in scripts are raw `u64`s mapped to `GlobalId::User`. Shards are referenced by a string alias; the first mention of an alias allocates a fresh `ShardId`.
