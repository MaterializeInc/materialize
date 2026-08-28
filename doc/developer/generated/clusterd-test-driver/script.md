---
source: src/clusterd-test-driver/src/script.rs
revision: cb4e73fb79
---

# mz-clusterd-test-driver::script

AST and parser for the text command script format.

`Command` is an enum covering all supported script directives: `CreateInstance`, `UpdateConfiguration`, `InitializationComplete`, `DefineSchema`, `WriteSingleTs`, `WriteSpread`, `WriteRows`, `DefineIndex`, `CreateDataflow`, `Explain`, `Peek`, `Count`, `Schedule`, `AllowCompaction`, `AllowWrites`, `AwaitFrontier`, `AwaitSubscribe`, `Reconnect`. Each variant carries its parsed parameters.

`Explain` renders a dataflow's lowered LIR plan as `EXPLAIN PHYSICAL PLAN`-style text without submitting a compute operation. Its `target` field is an `ExplainTarget`, which is either `Inline` (the full `create-dataflow` body given directly) or `Reference` (a name referring to a dataflow a prior `create-dataflow name=<name>` declared). The reference form avoids repeating the dataflow body just to assert its plan.

`CreateDataflow` records its spec in `ScriptState::dataflows` under the dataflow's name, so a later `Explain { target: Reference { name } }` can re-render the plan without repeating the body.

`CreateInstance` carries an `initial_config: Vec<ConfigSetting>` field (the create-time dyncfg snapshot the controller would supply), parsed from `name type value` body rows identical in format to those accepted by `update-configuration`. The snapshot is applied to the replica's worker config before create-time setup, so a scenario can assert that create-time work observes synced values rather than dyncfg defaults.

`ImportSpec`, `BuildSpec`, `ExportSpec`, `ExplainTarget`, `ColumnSpec`, `ConfigSetting` are supporting structs for the structured command bodies (MIR `define` sub-commands, schema definitions, dyncfg key-value pairs, and explain targets).

`ScriptState` is the mutable state threaded through a script run: it holds the `Driver`, persist client and location, named schemas, shard aliases, exported index registry, materialized-view outputs, named dataflow specs (for `explain ref=`), and the ephemeral-id counter for count-sugar dataflows.

Object IDs in scripts are raw `u64`s mapped to `GlobalId::User`, or carry an explicit `s`/`si`/`u`/`t` namespace prefix. Shards are referenced by a string alias; the first mention of an alias allocates a fresh `ShardId`.
