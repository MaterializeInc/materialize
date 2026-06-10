---
source: src/pgcopy/src/copy.rs
revision: e65e51d066
---

# mz-pgcopy::copy

Implements encoding and decoding of PostgreSQL COPY text, CSV, and binary formats.
`encode_copy_format` and `decode_copy_format` dispatch on `CopyFormatParams` (Text, Csv, Binary, Parquet) to per-format helpers; `CopyTextFormatParser` / `RawIterator` handle escape sequences for the text format; `CopyCsvFormatParams` and `CopyTextFormatParams` carry configurable delimiters, quotes, escape characters, and null sentinels.
`encode_copy_format_header` writes a CSV header row when requested.
CSV decoding uses `csv-core` rather than the higher-level `csv` crate so that per-field quote state is available; `DecodedField` records the `start..end` range into the decode output buffer and a `quoted` flag indicating whether the field's first input byte was the configured quote character. The NULL-marker and end-of-copy checks both require the field to be unquoted, preventing a quoted empty string (`""`) from silently decoding to SQL NULL and preventing a quoted `"\."` from being mistaken for the bare `\.` terminator.
