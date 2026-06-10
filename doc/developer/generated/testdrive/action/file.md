---
source: src/testdrive/src/action/file.rs
revision: 1a79aef641
---

# testdrive::action::file

Implements the `file-append` and `file-delete` builtin commands for writing test data to temporary files.
`run_append` writes (optionally compressed) content to a file under the testdrive temp directory, supporting bzip2, gzip, xz, zstd, and no compression.
`build_compression` parses the optional `compression` argument from a `BuiltinCommand` and returns a `Compression` enum value; it is re-used by the S3 action.
