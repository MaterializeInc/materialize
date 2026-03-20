# Simulation Tests

## Deterministic simulation failures are real bugs

The simulation tests in `persist_sim.rs` are seeded and deterministic. A given
seed must always produce the same trace and the same outcome. If a test fails
for a seed on one run but passes on a subsequent run, **that is not a flaky
test — it is a determinism bug**.

The entire value of DST depends on reproducibility. A "flaky" failure means
nondeterminism has leaked into the simulation: HashMap iteration order, tokio
scheduling across iterations, `Instant::now()` affecting control flow, shared
global state between seeds, or something similar. Every such failure must be
investigated and root-caused, not dismissed.

When investigating:

1. **Reproduce first**: `SEED=<n> cargo test -p mz-persist-shared-log persist_sim_single -- --nocapture`
2. **Read the trace**: the full operation history is printed on failure. Look at
   the last few operations before the mismatch.
3. **Check for inter-seed contamination**: does the failure depend on which seeds
   ran before it? Try `SEED=<n> SIM_SEEDS=1` vs running as part of a larger
   batch. If it only fails in the batch, something is leaking between iterations.
4. **Check for tokio nondeterminism**: the tests run on `current_thread` runtime,
   which should be deterministic within a single seed. But spawned tasks from
   prior seeds that haven't fully shut down can interfere. The
   `persist_sim_deterministic` test exists specifically to catch this.
5. **Never dismiss**: if you cannot reproduce a failure, add the seed to a
   tracked list and increase the `persist_sim_deterministic` iteration count
   around that seed range.

## Architecture

- `scenario.rs` — shared vocabulary (`SharedLogOp`, `SharedLogObservation`) and
  independent oracle (`SharedLogOracle`). The oracle implements Stateright's
  `SequentialSpec` trait so it can be used for both DST linearizability checking
  and (future) Stateright model checking.
- `trace.rs` — structured operation trace (`SimTrace`) printed on failure.
- `persist_sim.rs` — the simulation harness. Every operation is checked against
  the oracle and fed through Stateright's `LinearizabilityTester`.

## Running

```bash
# Single seed
SEED=42 cargo test -p mz-persist-shared-log persist_sim_single -- --nocapture

# Default (100 seeds)
cargo test -p mz-persist-shared-log persist_sim

# Extended
SIM_SEEDS=1000 cargo test -p mz-persist-shared-log persist_sim_single

# Infinite fuzzing
SEED=0 cargo test -p mz-persist-shared-log persist_sim_fuzz -- --ignored
```
