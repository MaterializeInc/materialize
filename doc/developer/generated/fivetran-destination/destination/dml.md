---
source: src/fivetran-destination/src/destination/dml.rs
revision: 849327076c
---

# mz-fivetran-destination::destination::dml

DML operations for the Fivetran destination: `truncate_table` and `write_batch`.

`handle_truncate_table` deletes rows soft-deleted before a given UTC timestamp (using `_fivetran_deleted` and `_fivetran_synced`).

`handle_write_batch` processes a `WriteBatchRequest` containing one or more batch files. Each file is read from disk, optionally decompressed (gzip via `GzipDecoder` or zstd via `ZstdDecoder`), optionally decrypted (AES-256-CBC via `AsyncAesDecrypter`), then parsed as CSV. Rows are applied as upserts or deletes based on the `_fivetran_deleted` column.

A `sha256_file` helper validates file integrity by SHA-256.
