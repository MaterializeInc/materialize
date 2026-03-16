---
source: src/testdrive/src/action/file.rs
revision: 90346e11f1
---

# testdrive::action::file

Implements the `file-append` and `file-delete` builtin commands for writing test data to temporary files.
`run_append` writes (optionally compressed) content to a file under the testdrive temp directory, supporting bzip2, gzip, xz, zstd, and no compression.
`build_compression` constructs the appropriate async compressor writer, and is re-used by the S3 action.
