---
source: src/storage/src/decode/csv.rs
revision: ddc1ff8d2d
---

# mz-storage::decode::csv

Implements `CsvDecoderState`, which wraps `csv_core::Reader` to decode byte chunks into `Row` values with configurable delimiter and optional header validation.
It handles column-count validation, UTF-8 checking, and header-row skipping, and is used as a variant of `DataDecoderInner` in `decode.rs`.
