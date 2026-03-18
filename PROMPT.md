Looks like there are bugs in the recently added subscribe-based frontend
read-then-write (RTW) implementation. Specifically, I want you do invetigate
why this test is failing and fix it.

bin/cargo-test -p mz-environmentd --test sql -- dont_drop_sinks_twice --nocapture

You might have to iterate and attempt multiple ways of fixing, re-running that
test, etc. At the end, when you fix the test run all the tests in that package
to ensure we didn't break anything. And if we did we need to fix that as well.

* Code should be simple and clean, never over-complicate things.
* At the end I want the minimal set of fixes, if we iterate and try multiple things, clean those up and fix only the minimum required to make all tests pass
* Each solid progress should be committed as a jj change
* Before committing, you should test that what you produced is high quality and that it works.
* Code should be very well commented, it needs to explain what is fixed and how and why
* At the end of this file, create a work in progress log, where you note what you already did, what is missing. Always update this log.
* Read this file again after each context compaction.

## Work-in-Progress Log

### Done
- Ran the failing test, identified the error: "internal error: subscribe channel closed" during INSERT INTO t1 SELECT generate_series(0, 10000)
- Root-caused three bugs in the subscribe-based frontend RTW implementation:
  1. **Timeline bug**: `validate_read_then_write` didn't include the target table in timeline determination. For constant SELECTs (like `generate_series`), this gave `NoTimestamp` → `as_of = Timestamp::MAX`, making the subscribe produce data at MAX and making writes invalid.
  2. **Id bundle bug**: The target table wasn't in the id_bundle for `determine_timestamp`, so `FreshestTableWrite` didn't consider the target table's write frontier.
  3. **Drain race**: The drain loop in the OCC loop treated `TryRecvError::Disconnected` as an error, but it's benign when the subscribe finishes (coordinator drops sender) between the last recv() and the drain.
- Also added a synthetic progress message in `process_response` (active_compute_sink.rs) for when a subscribe finishes (empty upper antichain). The empty antichain can't be represented as a single timestamp, so previously no progress was sent.
- Fixed test passes: `dont_drop_sinks_twice`

### In Progress
- Nothing remaining

### Verified
- `test_dont_drop_sinks_twice` passes
- `test_cancel_linearize_read_then_writes` passes individually; fails in full suite but this is a **pre-existing flaky test** (same failure on parent commit without any of our changes)
- All other tests in the full suite pass (6/7 pass, the 1 failure is the pre-existing flaky test above)

### Files Changed
- `src/adapter/src/active_compute_sink.rs` — synthetic progress for empty upper
- `src/adapter/src/frontend_read_then_write.rs` — timeline fix, id bundle fix, drain fix
