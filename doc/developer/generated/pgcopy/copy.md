---
source: src/pgcopy/src/copy.rs
revision: 95baa04a85
---

# mz-pgcopy::copy

Implements encoding and decoding of PostgreSQL COPY text, CSV, and binary formats.
`encode_copy_format` and `decode_copy_format` dispatch on `CopyFormatParams` (Text, Csv, Binary, Parquet) to per-format helpers; `CopyTextFormatParser` / `RawIterator` handle escape sequences for the text format; `CopyCsvFormatParams` and `CopyTextFormatParams` carry configurable delimiters, quotes, escape characters, and null sentinels.
`encode_copy_format` accepts a `TextEncodeSettings` argument that is forwarded to `Value::encode_text` for text and CSV formats; callers that encode outside a session context (such as `COPY TO <external destination>` in the dataflow layer) must pass `TextEncodeSettings::STABLE`.
`encode_copy_format_header` writes a CSV header row when requested; it uses `TextEncodeSettings::STABLE` internally.
CSV decoding uses `csv-core` rather than the higher-level `csv` crate so that per-field quote state is available; `DecodedField` records the `start..end` range into the decode output buffer and a `quoted` flag indicating whether the field's first input byte was the configured quote character. The NULL-marker and end-of-copy checks both require the field to be unquoted, preventing a quoted empty string (`""`) from silently decoding to SQL NULL and preventing a quoted `"\."` from being mistaken for the bare `\.` terminator. An `at_record_start` flag tracks whether the decoder is at the beginning of a record; when it is, any orphan `\r`/`\n` bytes (left by `csv-core` after consuming the `\r` half of a CRLF terminator) are skipped before inspecting the quote byte, preventing the orphan from being mistaken for the field's first character and misclassifying a quoted field as unquoted.
