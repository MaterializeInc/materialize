---
source: src/fivetran-destination/src/destination/dml.rs
revision: 2c0add6dcc
---

# mz-fivetran-destination::destination::dml

DML operations for the Fivetran destination: `handle_truncate_table` and `handle_write_batch`.

`handle_truncate_table` processes a `TruncateRequest`. When the `soft` field is absent it issues a hard `DELETE` on rows where the synced column is before the given UTC timestamp; when `soft` is present it issues an `UPDATE` that sets the deleted column to `true`. If the target table does not exist the operation is a no-op.

`handle_write_batch` processes a `WriteBatchRequest` and applies three ordered operation types against the destination table: replace, update, then delete. Fivetran requires this ordering.

- **replace**: Copies rows from each file into a scratch table, then deletes matching primary-key rows from the destination and re-inserts from the scratch table.
- **update**: For each row in each update file, issues an `UPDATE` statement that sets non-primary-key columns to the new value unless the field contains the "unmodified string" sentinel, in which case the existing column value is preserved.
- **delete**: Copies rows from each file into a scratch table, then marks matching rows in the destination as soft-deleted by setting `_fivetran_deleted = true` and updating `_fivetran_synced` to the maximum synced timestamp from the scratch table.

Files are opened by `load_file`, which first applies optional AES-256-CBC decryption (the initialization vector is stored in the first 16 bytes of the file) and then optional decompression (gzip via `GzipDecoder` or zstd via `ZstdDecoder`), before returning an `AsyncFileReader`.

The replace and delete paths both use a persistent scratch table in the `_mz_fivetran_scratch` schema. The scratch table name is derived from a SHA-256 hash of the fully qualified destination table name to stay within Materialize identifier length limits. `get_scratch_table` creates the scratch table if it does not exist, recreates it if the column schema no longer matches, or clears and reuses it otherwise. A `ScratchTableGuard` (marked `#[must_use]`) deletes all rows from the scratch table when the operation completes.

Rows are streamed from CSV files into the scratch table via the `COPY FROM STDIN` protocol using `copy_files`. Column order in the CSV is remapped to match the destination table schema through `AsyncCsvReaderTableAdapter`.

`TextFormatter` implements `tokio_postgres::types::ToSql` using the text wire format, mapping the configured null string to SQL `NULL` and passing all other values as raw bytes.
