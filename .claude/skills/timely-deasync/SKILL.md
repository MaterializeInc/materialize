---
name: timely-deasync
description: >
  This skill should be used when the user wants to write, modify, or understand
  a Timely Dataflow operator in Materialize. Trigger when the user mentions
  "Timely operator", "OperatorBuilder", "OperatorBuilderRc", "build_reschedule",
  "dataflow operator", "SyncActivator", "capability", "CapabilitySet",
  "InputHandle", "OutputBuilder", or when editing operator code that uses
  timely::dataflow::operators::generic. Also trigger when converting async
  operators to sync operators.
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

## Antichain partial order

`PartialOrder` for `Antichain<T>`: `a <= b` iff every element in `b` has some
element in `a` that is `<=` it. This means:

* `{0} <= {}` is **true** (vacuously -- no elements in `{}` to check)
* `{} <= {0}` is **false**
* The empty antichain `{}` is the **maximum** (most advanced frontier)
