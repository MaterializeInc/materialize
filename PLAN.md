# Plan: 0dt read-only support for logging persist sinks

## Problem

Logging persist sinks are not tracked as compute collections by the controller, so they cannot use the per-collection `AllowWrites` mechanism.
During 0dt deploys, a read-only replica should not write to persist shards until promoted.

An earlier attempt piggybacked on the first `AllowWrites` for any collection, but this broke clusters with no MVs (no `AllowWrites` was ever sent).

## Approach: add `read_only` to `InstanceConfig`

The compute controller already knows its `read_only` state (instance-level `read_only: bool`).
`CreateInstance(InstanceConfig)` is sent to all replicas and tracked in the command history.
Adding a `read_only: bool` field to `InstanceConfig` lets the controller communicate its read-only state to all replicas at instance creation time.

On the replica, `handle_create_instance` sets a `watch::Sender<bool>` on `ComputeState` before initializing logging, so logging persist sinks pick up the correct value.
The signal also flips to writable on the first `AllowWrites` command for any collection, providing a fallback for 0dt promotion.

This handles all cases:
* Read-only controller (0dt catchup): sends `read_only: true`, sinks stay read-only.
* Writable controller: sends `read_only: false`, sinks start writable.
* Writable controller with no MVs: still sends `read_only: false`, sinks start writable.
* Command history replay: the `CreateInstance` includes the `read_only` field, so reconnecting replicas get the right state.
* 0dt promotion: `AllowWrites` for any collection flips the signal from `true` to `false`.

## Implementation (done)

### 1. Add `read_only` field to `InstanceConfig`

**File:** `src/compute-client/src/protocol/command.rs`

* Added `pub read_only: bool` to `InstanceConfig`.
* Updated `compatible_with()` destructures to include `read_only: _` (transitions handled via `AllowWrites`, no compatibility check needed).

### 2. Set `read_only` from the compute controller

**File:** `src/compute-client/src/controller/instance.rs`

* In `Instance::run()`, set `read_only: self.read_only` when constructing `InstanceConfig`.

### 3. Add replica-level read-only signal to `ComputeState`

**File:** `src/compute/src/compute_state.rs`

* Added `read_only_tx: watch::Sender<bool>` and `pub read_only_rx: watch::Receiver<bool>` fields.
* Initialized as `watch::channel(true)` (read-only by default, safe default for 0dt).
* In `handle_create_instance`, call `send_replace(config.read_only)` before `initialize_logging`.
* In `handle_allow_writes`, flip `read_only_tx` to `false` via `send_if_modified`.

### 4. Use the replica-level signal in logging persist sinks

**File:** `src/compute/src/logging/persist.rs`

* Replaced `let (_tx, read_only_rx) = watch::channel(false)` with `compute_state.read_only_rx.clone()`.
* Removed `use tokio::sync::watch` import.
