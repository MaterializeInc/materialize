---
source: src/persist/src/file.rs
revision: a298f30bbb
---

# persist::file

Implements the `Blob` trait using a local filesystem directory, primarily for testing and benchmarking.
Writes are atomic: data is first written to a `.tmp` file and then renamed into place with `fsync` calls on both the file and parent directory.
When the `tombstone` flag is set, `delete` renames to a `.bak` file instead of removing it, and `restore` reverses that rename.
Forward slashes in blob keys are replaced with U+2215 (DIVISION SLASH) to keep the backing store as a flat directory.
