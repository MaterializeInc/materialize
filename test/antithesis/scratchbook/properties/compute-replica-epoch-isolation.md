# compute-replica-epoch-isolation

## Summary
Compute replica incarnations are isolated by epoch — commands from old epochs cannot execute after a new epoch starts.

## Evidence

### Code Paths
- `src/compute-client/src/controller/replica.rs:70-107` — Epoch at line 93, ReplicaTask at line 146
- `src/compute-client/src/protocol/command.rs:45-54` — Hello command with nonce for protocol iteration
- `src/compute-client/src/controller/replica.rs:142-144` — Task abortion on rehydration clears old commands

### How It Works
Each replica incarnation gets a unique epoch (nonce + u64). On rehydration, the controller aborts the old ReplicaTask and creates a new one with an incremented epoch. The Hello command includes the new nonce, and the replica rejects commands with mismatched nonces.

### What Goes Wrong on Violation
Stale commands from a previous incarnation execute on the new replica, causing it to diverge from the coordinator's expected state. Query results become inconsistent across replicas.

### SUT-Side Instrumentation Notes
- No existing Antithesis assertions
- Candidate: On command receipt, add `assert_always!(command.epoch >= current_epoch)` in the replica's command handler
- Candidate: After rehydration, add `assert_reachable!` that the new epoch is used for the first command

### Provenance
Surfaced by Distributed Coordination focus.
