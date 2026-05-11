# strict-serializable-reads

## Summary
Reads respect the timestamp oracle's linearization point — later reads see all changes visible to earlier reads.

## Evidence

### Code Paths
- `src/adapter/src/coord/timestamp_selection.rs:40-52` — When `chosen_ts` differs from `oracle_ts`, peek results must be delayed until oracle catches up
- `src/adapter/src/coord/sequencer/inner.rs:2097-2116` — Strict serializable reads tracked via `strict_serializable_reads_tx`
- `src/adapter/src/coord/timestamp_selection.rs:228-240` — `needs_linearized_read_ts` check
- `src/adapter/src/coord/in_memory_oracle.rs:92-101` — Oracle timestamp advancement

### How It Works
The coordinator assigns every read a timestamp from the oracle. The oracle maintains a monotonically advancing timestamp. Strict serializable reads wait for the oracle to confirm their timestamp is linearized before returning results. This ensures no read can see a state "in the past" relative to another concurrent read.

### What Goes Wrong on Violation
Users observe non-repeatable reads: query A at time T sees data that query B at time T+1 does not see. This violates the strict serializability contract that is Materialize's primary differentiator from other streaming systems.

### Workload-Level Verification
This property is best verified at the workload level:
1. Client A writes row R and receives acknowledgment
2. Client B reads and must see R (or a later state including R)
3. Client C reads and must see at least what B saw

The workload checks SQL results, not internal state.

### SUT-Side Instrumentation Notes
- No existing Antithesis assertions
- Candidate: `timestamp_selection.rs` oracle advancement — add `assert_always!` that oracle timestamp never decreases
- Candidate: After peek response, add workload-side `Always` assertion comparing read timestamp ordering with data ordering

### Provenance
Surfaced by Protocol Contracts focus (merged from timestamp-oracle-linearization and strict-serializable-ordering).
