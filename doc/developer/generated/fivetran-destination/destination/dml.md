---
source: src/fivetran-destination/src/destination/dml.rs
revision: 82d92a7fad
---

# mz-fivetran-destination::destination::dml

Implements the three DML batch operations required by the Fivetran SDK: `handle_truncate_table` soft- or hard-deletes rows older than a given timestamp; `handle_write_batch` orchestrates replace, update, and delete file operations in the required order.
Replace and delete operations use a per-table scratch table in `_mz_fivetran_scratch` to stage CSV data via `COPY FROM STDIN` before merging into the destination.
Update operations issue individual parameterized `UPDATE` statements per row using `CASE … WHEN unmodified_string THEN … END` to skip unchanged columns.
Compressed (Gzip, Zstd) and AES-256-CBC encrypted files are transparently decoded by the `crypto` and async-compression layers.
