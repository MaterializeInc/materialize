# command-channel-ordering

## Summary
Timely workers must see CreateDataflow commands in identical order — code explicitly acknowledges this is not guaranteed by Timely.

## Evidence

### Code Paths
- `src/compute/src/command_channel.rs:88-90` — Comment: "relies on Timely channels preserving order of inputs, which is not something they guarantee"
- `src/compute/src/command_channel.rs:96-100` — Source operator activation sequence
- `src/compute/src/command_channel.rs:41-58` — Sender using `Arc<Mutex>` activator

### How It Works
The command channel broadcasts commands from worker 0 to all other Timely workers via a Timely dataflow operator. Commands are fed in order, but the code explicitly notes that Timely does not guarantee preservation of input ordering.

### What Goes Wrong on Violation
Workers execute dataflows in different orders, causing divergent state. Since all workers must agree on dataflow state for correct results, reordering leads to inconsistent query results or panics during distributed computation.

### Why This Is an Antithesis Target
This is the kind of bug that almost never manifests in normal testing because thread scheduling is usually consistent. Antithesis's deterministic scheduling exploration can systematically vary worker activation timing to expose reordering.

### SUT-Side Instrumentation Notes
- No existing Antithesis assertions
- Candidate: On each worker, log the command sequence and add `assert_always!` that worker N's command sequence matches worker 0's
- This is a strong candidate for SUT-side instrumentation since the invariant is internal to the compute engine

### Provenance
Surfaced by Concurrency focus.
