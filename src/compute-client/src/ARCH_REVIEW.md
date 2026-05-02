# ARCH_REVIEW: mz-compute-client

## Finding 1 — `as_of_selection` is misplaced (acknowledged debt)

**Location:** `src/as_of_selection.rs` (1,340 LOC)

The module's own doc states it "should be part of the controller and transparent
to the coordinator, but that's difficult to reconcile with the current controller
API." Bootstrap-time as-of selection is a compute implementation concern but is
invoked by the coordinator because the API can't express it yet. This is a known
seam leak; the module is a migration candidate once the controller API is
extended.

**Risk:** Low today; risk grows if more bootstrap logic accumulates here by
convention rather than design.

## Finding 2 — Read-capability contract enforced by convention, not types

**Location:** `src/compute-client/src/controller/instance.rs:2579`

`SharedCollectionState.read_capabilities` is `Arc<Mutex<MutableAntichain<Timestamp>>>`.
The comment says only three methods may modify it, and there is an open TODO to
"restructure the code to enforce the above in the type system." Currently any
code with a `&SharedCollectionState` or clone of the `Arc` can modify the
antichain without going through the sanctioned path.

**Risk:** Medium — read-hold invariants are correctness-critical; a future
refactor touching this struct could violate them silently.

## Finding 3 — `SequentialHydration` assumes single-export dataflows

**Location:** `src/compute-client/src/controller/sequential_hydration.rs:24-26`

The shim that enforces hydration concurrency relies on the assumption that each
dataflow has exactly one collection export. The assumption is documented but not
asserted structurally. If multi-export dataflows are introduced, `Schedule`
command routing may become incorrect.

**Risk:** Low currently; medium if dataflow structure evolves.
