---
source: src/storage/src/decode/csv.rs
revision: 5209db5b09
---

# mz-storage::decode::csv

Implements `CsvDecoderState`, which wraps `csv_core::Reader` to decode byte chunks into `Row` values with configurable delimiter and optional header validation.
It handles column-count validation, UTF-8 checking, and header-row validation (skipping the header after verifying column names match, and propagating any parse error on the header row rather than swallowing it), and is used as a variant of `DataDecoderInner` in `decode.rs`.
