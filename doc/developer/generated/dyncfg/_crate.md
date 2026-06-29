---
source: src/dyncfg/src/lib.rs
revision: a8f4526d28
---

# mz-dyncfg

Provides a lightweight, type-safe runtime configuration system that allows individual `Config` values to be declared near their usage site, registered into a shared `ConfigSet`, and updated at runtime without process restarts.

## Purpose

The crate minimizes boilerplate for dynamic configuration: a `Config<T>` is declared as a `const`, registered once per process into a `ConfigSet`, and then read anywhere via `Config::get`.
Multiple independent `ConfigSet`s can coexist (e.g. one per unit test), so tests do not interfere with each other or with the process-wide configuration.
`ConfigUpdates` provides a serializable batch of value changes that can be propagated across process boundaries (e.g. from `environmentd` to `clusterd`).

## Module structure

The crate is a single `lib.rs`.
The `impls` private submodule contains `ConfigType` and `From<T>` implementations for the concrete types `bool`, `u32`, `usize`, `Option<usize>`, `f64`, `String`, `Option<String>`, `Duration`, and `serde_json::Value`.

## Key types

* `Config<D>` — a named, typed configuration handle; constructed as a `const` with a default value and description. The optional `.scoped(ParameterScope)` builder method declares the scope at which the parameter may be overridden (defaults to `ParameterScope::Environment`).
* `ParameterScope` — enum (`Environment`, `Cluster`, `Replica`) declaring the override granularity for a `Config`. `Environment` (the default) means no cluster/replica overrides; `Cluster` allows per-cluster overrides resolved at plan time; `Replica` allows per-replica overrides resolved at the controller's per-replica dyncfg push. Exposes `as_str()` and `DEFAULT` const.
* `ConfigSet` — a registry of `Config` values; cloning shares the underlying atomics so updates are visible to all holders of a clone.
* `ConfigEntry` — the storage record for one config inside a `ConfigSet`, exposing name, description, scope, default, and current value. `scope()` returns the `ParameterScope`; `parse_val(val: &str)` parses a string into a `ConfigVal` of the entry's type (type-erased analog of `Config::parse_val`).
* `ConfigVal` — type-erased enum used for storage and serialization; variants mirror the supported `ConfigType` implementations.
* `ConfigValHandle<T>` — a pre-looked-up, cheaply-cloneable handle that amortizes the name-lookup cost on hot paths.
* `ConfigUpdates` — a serializable `BTreeMap<String, ConfigVal>` batch; `apply` writes all values into a target `ConfigSet`, skipping unknown names.

## Internals

`ConfigValAtomic` backs each `ConfigEntry` with lock-free atomics (`AtomicBool`, `AtomicU32`, etc.) for scalar types and `Arc<RwLock<T>>` for heap types, enabling concurrent reads without a global lock.
The `serde_json_string` private module serializes `Json` variant values as strings for compatibility with non-self-describing formats such as bincode.

## Dependencies

* `mz-ore` — test utilities.
* `humantime` — parses `Duration` values from human-readable strings (e.g. `"5 s"`).
* `serde` / `serde_json` — serialization of `ConfigVal` and `ConfigUpdates`.
* `tracing` — error logging when an update targets an unknown config.

## Downstream consumers

Used pervasively across Materialize components wherever a tunable parameter should be adjustable at runtime; `dyncfg-file` extends this crate to load values from a file.
