# Lean 4 compute protocol semantics

A mechanized model of Materialize's compute protocol control-plane
contract: the staging handshake and per-command/per-response legality
rules documented in `src/compute-client/src/protocol/{command,
response}.rs`.

Read `compute-protocol.md` first for the reading order and a
cross-reference from doc-comment sentence to Lean theorem.

This is Phase 1 of a three-phase plan (see
`doc/developer/design/20260724_compute_protocol_semantics.md`): only
staging and command/response legality are modeled here. Read-hold
lifecycle against `AllowCompaction` and multi-replica/worker command
sharing are later phases, not yet started.

## Relationship to MaterializeInc/materialize#36614

That (at time of writing, unmerged) PR builds a different Lean 4
model in this same directory: `Mz`, covering scalar evaluation and
collection semantics (the data plane), under Lean namespace `Mz`.
This directory's `MzCompute` library covers the compute protocol (the
control plane) instead, under its own namespace, and was bootstrapped
independently (its own `Dockerfile`/`lakefile.toml`/CI script) because
`doc/developer/semantics/` did not yet exist on this codebase's
history when this work started. If #36614 lands, the two efforts
reconcile into one `lakefile.toml` with two `lean_lib` targets sharing
one Docker image. See that PR's design doc and this directory's for
each library's own scope.

## Build

`ci/test/lean-semantics.sh` builds the `mz-lean-semantics` Docker
image and runs `lake build` inside it. For one-shot probes (`lake env
lean`, inspecting a `sorry`'s goal state, and similar), use
`bin/in-image` once the image exists.
