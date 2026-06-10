---
source: src/repr/src/row/encode.rs
revision: 95ba315ab3
---

# mz-repr::row::encode

Provides `RowColumnarEncoder` and `RowColumnarDecoder` for encoding/decoding `Row`s into Apache Arrow columnar format using the `mz-persist-types` columnar codec infrastructure.
The `preserves_order` function reports whether columnar encoding preserves lexicographic ordering for a given `RelationDesc`, enabling sorted columnar storage optimizations.
