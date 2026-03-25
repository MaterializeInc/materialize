---
source: src/pgcopy/src/copy.rs
revision: 59358e137b
---

# mz-pgcopy::copy

Implements encoding and decoding of PostgreSQL COPY text, CSV, and binary formats.
`encode_copy_format` and `decode_copy_format` dispatch on `CopyFormatParams` (Text, Csv, Binary, Parquet) to per-format helpers; `CopyTextFormatParser` / `RawIterator` handle escape sequences for the text format; `CopyCsvFormatParams` and `CopyTextFormatParams` carry configurable delimiters, quotes, escape characters, and null sentinels.
`encode_copy_format_header` writes a CSV header row when requested.
