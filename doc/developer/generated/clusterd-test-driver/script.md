---
source: src/clusterd-test-driver/src/script.rs
revision: 6d4c0fbb2b
---

# mz-clusterd-test-driver::script

AST and parser for the text command script format.

`Command` is an enum covering all supported script directives: `CreateInstance`, `UpdateConfiguration`, `InitializationComplete`, `DefineSchema`, `WriteRowsSingleTs`, `WriteRowsSpread`, `Define`, `DefineIndex`, `Peek`, `AwaitFrontier`, `AwaitSubscribe`, `Reconnect`, and others. Each variant carries its parsed parameters.

`ImportSpec`, `BuildSpec`, `ExportSpec`, `ColumnSpec`, `ConfigSetting` are supporting structs for the structured command bodies (MIR `define` sub-commands, schema definitions, dyncfg key-value pairs).

Object IDs in scripts are raw `u64`s mapped to `GlobalId::User`. Shards are referenced by a string alias; the first mention of an alias allocates a fresh `ShardId`.
