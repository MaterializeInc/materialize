For the `KEY FORMAT <key_fmt> VALUE FORMAT <value_fmt>`, the following table specifies the format and envelope compatibility:

| Format | Append-only envelope | Upsert envelope | Debezium envelope |
|--------|:--------------------:|:---------------:|:-----------------:|
| Avro              | ✓         | ✓               | ✓                 |
| JSON/Text/Bytes   | ✓         | ✓
| CSV               | ✓         |                 |
