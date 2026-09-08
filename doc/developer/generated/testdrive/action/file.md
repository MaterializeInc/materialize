---
source: src/testdrive/src/action/file.rs
revision: 5b2cefc829
---

# testdrive::action::file

Implements the `file-append` and `file-delete` builtin commands for writing test data to temporary files.
`run_append` writes (optionally compressed) content to a file whose location is determined by an optional `container` argument: no container uses the testdrive temp directory, and `container=fivetran` places the file under the Fivetran destination files path from `State`. Supported compression formats are bzip2, gzip, xz, zstd, and no compression.
`build_compression` parses the optional `compression` argument from a `BuiltinCommand` and returns a `Compression` enum value; it is re-used by the S3 action.
