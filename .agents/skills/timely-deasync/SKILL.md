---
name: timely-deasync
description: >
  This skill should be used when the user wants to write, modify, or understand
  a Timely Dataflow operator in Materialize. Trigger when the user mentions
  "Timely operator", "OperatorBuilder", "OperatorBuilderRc", "build_reschedule",
  "dataflow operator", "SyncActivator", "capability", "CapabilitySet",
  "InputHandle", "OutputBuilder", or when editing operator code that uses
  timely::dataflow::operators::generic. Also trigger when converting async
  operators to sync operators ("deasync"), when an operator drops or misorders
  data, when writing a test harness or regression/fuzz tests for an operator,
  or when debugging a stalled output frontier.
---

# Writing Timely operators in Materialize

## Core rules

* **All workers must construct the same dataflow graph.** Every worker must create
  every operator and every stream, even if only one worker does useful work. Rendering
  an operator on a subset of workers causes crashes or hangs because Timely expects
  matching progress messages from all workers. To restrict work to one worker, build the
  operator on all workers but have non-active workers skip the work (see "Single-worker
  operators" below).
* Operators must drain all their inputs every invocation.
  An operator that fails to drain an input will stall the dataflow.
* Operators hold capabilities that reserve the right to produce data at a given time.
  Retaining a capability prevents the output frontier from advancing past that time.
  Drop or downgrade capabilities promptly to allow downstream progress.
* Returning from a schedule closure does NOT prevent it from being scheduled again.
  To stop an operator, drop all capabilities and inputs.

## OperatorBuilderRc

`OperatorBuilderRc` is the sync operator builder used in Materialize.
It wraps Timely's `OperatorBuilder` with Rc-based output handles.

```rust
let mut builder = OperatorBuilderRc::new(name, scope.clone());
let info = builder.operator_info();

// Create outputs before inputs (no input connections yet).
let (output, output_stream) = builder.new_output();
let mut output = OutputBuilder::from(output);

// Inputs. Use new_input for default connections, new_input_connection for custom.
let mut input = builder.new_input(stream, Pipeline);

builder.build(move |capabilities| {
    // Constructor closure: runs once. Set up state here.
    let [cap]: [_; 1] = capabilities.try_into().expect("one capability per output");
    let mut cap_set = CapabilitySet::from_elem(cap);

    // Return the schedule closure.
    move |frontiers| {
        // Schedule closure: runs each activation.
        // frontiers[i] corresponds to input i.
        input.for_each(|cap, data| {
            output.activate().session(&cap).give_container(data);
        });
    }
});
```

### Frontier timing in build_reschedule

`build_reschedule` (which `build` delegates to) drains frontier changes FROM the
progress tracker at the START of each scheduling. Capability drops within the
closure are collected and propagated AFTER the closure returns, visible in the
NEXT scheduling.

### Disconnected inputs/outputs

Use `new_input_connection` with an empty slice to disconnect an input from all outputs
(the input won't influence output frontiers):

```rust
let mut input = builder.new_input_connection(stream, Pipeline, []);
```

Use `new_output_connection` to disconnect an output or connect it to specific inputs:

```rust
let (output, stream) = builder.new_output_connection([(2, Antichain::from_elem(Default::default()))]);
```

## Single-worker operators

Some operators must run on exactly one worker (e.g., to mint globally unique
batch descriptions). Select an active worker deterministically:

```rust
let active_worker_id = usize::cast_from(sink_id.hashed()) % scope.peers();
```

Non-active workers drop all capabilities immediately and skip all work:

```rust
builder.build(move |capabilities| {
    let mut state = None;
    if worker_id == active_worker_id {
        // Set up channels, spawn Tokio tasks, etc.
        state = Some(ActiveState { ... });
    }

    move |frontiers| {
        let Some(state) = &mut state else {
            // Non-active: drain inputs (mandatory!) but do nothing else.
            input.for_each(|_cap, _data| {});
            return;
        };
        // Active worker logic...
    }
});
```

Consolidate the active-worker check into one block before `builder.build()` to
avoid repeating `if worker_id == active_worker_id` inside the schedule closure.

## Converting async operators to sync + Tokio task

When an operator uses `AsyncOperatorBuilder` but its async work is I/O (persist,
channels), convert to `OperatorBuilderRc` + a spawned Tokio task. This removes
async overhead from the Timely thread.

If the operator's only awaits are on its timely input handles (no real I/O), no
task is needed — see "Converting async operators whose awaits are only on
inputs" below; that case is semantic, not mechanical.

### Pattern

1. **Spawn a Tokio task** before `builder.build()` that owns the async resources
   (WriteHandle, ReadHandle, etc.).
2. **Communicate via channels**: `mpsc::unbounded_channel` for commands (operator->task)
   and responses (task->operator).
3. **Use ArcActivator** for task->operator wakeups (Send-safe):
   ```rust
   let (activator, activation_ack) = ArcActivator::new(&scope, &info);
   // Move activator to Tokio task, keep activation_ack in operator.
   // Task calls activator.activate() after sending a response.
   // Operator calls activation_ack.ack() at start of each scheduling.
   ```
4. **Use SyncActivator** for simpler wakeups (one-shot or frontier watches):
   ```rust
   let sync_activator = scope.sync_activator_for(info.address.to_vec());
   // Task calls sync_activator.activate() to wake the operator.
   ```

### Task handle lifetime -- critical pitfall

`move` closures only capture variables they reference. A task handle assigned in
the constructor closure but not referenced in the schedule closure will be dropped
when the constructor returns, aborting the task immediately.

**Wrong:**
```rust
builder.build(move |capabilities| {
    let task_handle = spawn_task();
    let _keep = task_handle; // Dropped when constructor closure returns!

    move |frontiers| {
        // task_handle is NOT captured -- it was never referenced here.
    }
});
```

**Correct:**
```rust
builder.build(move |capabilities| {
    let task_handle = spawn_task();

    move |frontiers| {
        let _ = &task_handle; // Forces capture, handle lives as long as the closure.
    }
});
```

Alternatively, spawn before `builder.build()` and capture in the schedule closure
with the same `let _ = &task_handle` pattern.

### Channel error handling

* Use `.expect("task unexpectedly gone")` on `cmd_tx.send(...)`, not `let _ =`.
  Silent swallowing hides task crashes and causes subtle hangs.
* On `resp_rx.try_recv()`, check `Disconnected` explicitly and panic -- the task
  should not disappear while the operator is alive.

### Shutdown signals via channels

For tasks that watch an external frontier (e.g., persist upper), send the empty
frontier as the final message before exiting:

```rust
// Tokio task:
loop {
    writer.wait_for_upper_past(&frontier).await;
    frontier = writer.upper().clone();
    if tx.send(frontier.clone()).is_err() { return; }
    if activator.activate().is_err() { return; }
    if frontier.is_empty() { return; }  // Shutdown signal
}
```

On the operator side, wrap the receiver in `Option` and take it on empty frontier:

```rust
fn drain_rx(&mut self) {
    let Some(mut rx) = self.rx.take() else { return };
    loop {
        match rx.try_recv() {
            Ok(frontier) => {
                let done = frontier.is_empty();
                self.apply(frontier);
                if done { return; } // Drop rx -- task is done
            }
            Err(TryRecvError::Empty) => {
                self.rx = Some(rx); // Put back
                return;
            }
            Err(TryRecvError::Disconnected) => {
                panic!("task unexpectedly gone");
            }
        }
    }
}
```

### Logging cleanup on drop

When a Tokio task owns state that participates in introspection logging (e.g.,
correction buffers that log BatchEvent/DropEvent), aborting the task is async --
destructors may not run before the operator's logging state is dropped.

Track net counts (batches, records) on the Timely side as events flow through.
On drop, retract any outstanding counts:

```rust
impl Drop for MyLogger {
    fn drop(&mut self) {
        self.drain_channel();  // Get whatever arrived
        // Retract remaining counts not balanced by drop events
        for i in 0..self.net_batches {
            self.differential_logger.log(DropEvent {
                operator: self.operator_id,
                length: if i == 0 { self.net_records as usize } else { 0 },
            });
        }
        // Fire operator-level cleanup
        self.compute_logger.log(&ComputeEvent::ArrangementHeapSizeOperatorDrop(...));
    }
}
```

### Resource lifetime across operator boundaries

When a Tokio task owns a resource (e.g., a leased reader with SeqNo hold) that
must outlive work performed by a *different* operator (e.g., a fetch operator
downloading parts protected by that lease), share the task's `AbortOnDropHandle`
via `Rc` between both operators:

```rust
let shared_handle: Rc<RefCell<Option<AbortOnDropHandle<()>>>> =
    Rc::new(RefCell::new(None));

// Return operator holds an Rc clone -- keeps task alive until all fetches complete.
{
    let handle = Rc::clone(&shared_handle);
    let mut builder = OperatorBuilderRc::new("return_op", scope.clone());
    let mut input = builder.new_input_connection(feedback_stream, Pipeline, vec![]);
    builder.build(move |_caps| {
        move |frontiers| {
            let _ = &handle; // Prevent drop until this operator exits.
            input.for_each(|_cap, _data| {});
        }
    });
}

// Producer operator spawns task, stores handle in shared Rc.
*shared_handle.borrow_mut() = Some(task_handle);
```

The return operator exits only when the feedback frontier reaches the empty
antichain (all downstream work is done), guaranteeing the task stays alive.

**Pitfall:** Without this, the task (and its resource) can be aborted as soon as
the producer operator drops its handle, while the downstream operator still has
in-flight work that depends on the resource.

### Per-flight capability tracking for feedback loops

When a sync fetch operator needs to prevent a feedback frontier from advancing
until each fetch completes, retain per-item capabilities for both outputs:

```rust
// Send part to task, retain caps for both fetched_output and completed_fetches_output.
inflight_caps.push_back((cap.retain(0), cap.retain(1)));

// When fetch completes, pop and drop:
let (cap, _feedback_cap) = inflight_caps.pop_front().unwrap();
fetched_output.activate().session(&cap).give(result);
// _feedback_cap is dropped here, allowing the feedback frontier to advance.
```

This is stricter than the async operator pattern (where Timely implicitly tracks
the input->output frontier relationship) but guarantees that leases are not
released before the fetched data is emitted.

### Minimize channel hops for high-throughput data

Sending every data batch through a channel to a Tokio task adds per-batch
overhead (atomics, potential cache-line bouncing, wakeup cost). For operators
that process high volumes of small batches:

* Keep data buffers (e.g., correction buffers) on the Timely thread.
* Only send the final assembled work item (e.g., a `WriteBatch` command with
  collected updates) to the Tokio task.
* This avoids `Desired`, `Persist`, `PersistFrontier` etc. commands per
  activation -- only the actual async I/O crosses the channel boundary.

## Converting async operators whose awaits are only on inputs

If the operator's only `.await`s are on `AsyncInputHandle::next()`, convert to a
plain `OperatorBuilderRc` schedule closure — no Tokio task. This conversion is
NOT mechanical: two Materialize attempts (#36537, #36548) were reverted because
the rewrite silently changed semantics. Derive the semantics first.

### Derive the blocking structure first

An async operator awaits exactly one input at any moment. Find the predicate
that selects which input it blocks on (e.g. `remap.physical_upper <=
cap.time()` in txn-wal's `txns_progress_frontiers`). That predicate plus the
await placement ARE the operator's spec: they determine which events the
operator reacts to in which state. Write them down before writing sync code.

The sync API cannot replay the async event order: `for_each` yields only data,
and `frontiers[i]` is a post-activation snapshot. The Data/Progress
interleaving the async loop consumed one-at-a-time is gone — do not try to
reconstruct it. Instead, order the per-activation work so each step is safe by
a provable fact:

1. **Emit buffered input data at the pre-activation capability, BEFORE any
   downgrade.** Capabilities only move forward, so every buffered record has
   time `>= cap.time()`; emitting at the old cap always preserves `send_time <=
   record_time`. Emitting after a downgrade (including a `capability = None`
   shutdown for `until`) silently drops or misorders data — this exact reorder
   was the SQL-299 data loss in #36537.
2. **Advance the capability past an input frontier only under a protocol
   emptiness contract** (e.g. `DataRemapEntry` guarantees `[physical_upper,
   logical_upper)` is empty of writes). Name the contract in a comment; without
   it the downgrade strands data.

### Folding vs stepwise event processing

An async loop that handles events one at a time (apply, re-check predicate,
continue) collapses in sync to an eager fold over everything buffered. Folding
skips intermediate states (e.g. intermediate `logical_upper` downgrades).
Decide whether the skip is a granularity difference (frontier reaches the same
final value) or a correctness difference (data emitted at the wrong time)
before accepting it.

### Document intentional divergences

The async original may itself be buggy (e.g. dropping remap state when the
input closes, stalling the frontier — PER-4). The sync port may deliberately
diverge; record every divergence in a comment and the design doc, or a future
reader will "fix" it back.

### Preserve the two-phase shutdown contract

If the async operator returned a `PressOnDropButton` (it participates in
coordinated multi-worker shutdown — e.g. `txns_progress` returns
`Vec<PressOnDropButton>`), the sync port MUST reproduce `builder_async`'s
two-phase shutdown. The naive port — drop the capability as soon as
`shutdown_handle.local_pressed()` — is WRONG: this worker stops contributing to
the downstream frontier while other workers' instances still feed downstream,
so the frontier can advance past times whose input this worker has discarded
(cross-worker teardown skew).

`OperatorBuilderRc::build`'s plain schedule closure cannot express this. Use
`build_reschedule` (returns `bool`: `true` = reschedule me, `false` = done) and
mirror what `builder_async` does internally:

```rust
let (mut shutdown_handle, shutdown_button) = button(scope, info.address);
builder.build_reschedule(move |caps| {
    // ... set up state, create `button` via mz_timely_util::builder_async::button ...
    move |frontiers| {
        if shutdown_handle.local_pressed() {
            return if shutdown_handle.all_pressed() {
                // ALL workers pressed: now safe to drop caps and drain inputs.
                capability = None;
                input.for_each(|_cap, _data| {});
                false
            } else {
                // Local press only: WEDGE. Keep capabilities, leave inputs
                // undrained (their pending messages hold the frontier),
                // reschedule until the rest catch up.
                true
            };
        }
        // ... normal logic ...
        false // keep the operator alive
    }
});
(stream, shutdown_button.press_on_drop())
```

`button` needs `&mut shutdown_handle` for `all_pressed()` (it drains the
cross-worker channel). If the operator does NOT return a button (shutdown is
just "drop caps and inputs"), the plain `build` is fine — this only applies to
coordinated-shutdown operators.

## Testing operator conversions

Build a scripted-schedule harness: feed synthetic inputs via
`scope.new_input()`, play actions (send record, advance frontier, close input,
step worker), capture the output stream, probe the final frontier.

### Harness pitfalls

* Hold the operator's `PressOnDropButton` for the whole run. Binding it to
  `_button` inside the dataflow closure drops it at construction — pressing
  shutdown before the first activation.
* Closing an input means DROPPING the handle. `advance_to(u64::MAX)` is a
  finite frontier, not the empty antichain; close-detection logic never fires.
* `handle.send(x)` buffers; delivery happens on the next `advance_to`/`flush`.
  Sent data is not visible to the operator in the same step.
* Create the output before disconnected inputs: `new_output()` connects to all
  inputs existing at call time, so an input created first silently connects and
  breaks manual capability management.
* Drain to quiescence by stepping until the probe frontier is stable for
  several consecutive steps, with a panic cap (`assert!(step < N)`). A fixed
  step count silently masks non-termination.

### Tests must have teeth

For every bug class the conversion fixes or could reintroduce, write a targeted
regression test, then PROVE it: revert the fix in the operator, confirm the
test fails, restore. A regression test that passes against the broken operator
is worse than none.

### Fuzz with oracle-free properties; do NOT diff against the async impl

Fuzz random action schedules and assert direct properties of the sync operator:

* No data loss or duplication: emitted payload multiset equals sent payload
  multiset (sound whenever the schedule never legitimately shuts the operator
  down — use unique payloads).
* No premature shutdown: with `until = {}` and the data input never closed, the
  output frontier stays finite.

Do NOT use the old async impl as a differential oracle on random schedules. Its
blocked-on-one-input structure strands buffered data whenever a finite schedule
ends (or violates a protocol contract) while it awaits the other input,
producing spurious divergences. The tempting fix — weakening the assertion to
`async ⊆ sync` — trusts whichever impl emits more and hides real bugs. A
failing differential test means STOP and analyze which side is wrong; never
weaken the comparison to make it pass.

### Existing tests that assert exact emission timestamps

After a deasync, per-batch stream timestamps depend on scheduling cadence and
are not contractual. Relax such tests to assert (a) the exact record multiset
and (b) the differential invariant `stream_ts <= record_time`, not per-batch
timestamps. (Pattern: `as_of_until` in txn-wal, #36810.)

## Antichain partial order

`PartialOrder` for `Antichain<T>`: `a <= b` iff every element in `b` has some
element in `a` that is `<=` it. This means:

* `{0} <= {}` is **true** (vacuously -- no elements in `{}` to check)
* `{} <= {0}` is **false**
* The empty antichain `{}` is the **maximum** (most advanced frontier)
