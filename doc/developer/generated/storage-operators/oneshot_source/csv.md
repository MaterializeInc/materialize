---
source: src/storage-operators/src/oneshot_source/csv.rs
revision: ccd84bf8e8
---

# storage-operators::oneshot_source::csv

Implements `OneshotFormat` for CSV as `CsvDecoder`, with `CsvWorkRequest` and `CsvRecord` as its associated work and chunk types.
Because CSV cannot easily be parallelized, `split_work` always returns a single work request per object; `fetch_work` wraps the byte stream in optional decompression (gzip, bzip2, xz, zstd) and yields records; `decode_chunk` converts each record into a `Row` using the target table's column types.
