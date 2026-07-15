## `mz_foo`

The `mz_foo` view describes foos in the [dataflow] layer.

It has more detail.

<!-- RELATION_SPEC test_schema.mz_foo -->
| Field  | Type       | Meaning                                       |
|--------|------------|-----------------------------------------------|
| `id`   | [`uint8`]  | The ID. Corresponds to [`mz_bar.id`](#mz_bar). |
| `name` | [`text`]   | The name of the [dataflow].                   |

<!-- RELATION_SPEC_UNDOCUMENTED test_schema.mz_foo_per_worker -->

## `mz_baz`

The `mz_baz` source is special.

<!-- RELATION_SPEC test_schema.mz_baz NO_COMMENTS -->
| Field | Type      | Meaning |
|-------|-----------|---------|
| `x`   | [`text`]  | ex      |

See [more](/somewhere) for details.

[`uint8`]: /sql/types/uint8
[`text`]: /sql/types/text
[dataflow]: /get-started/arrangements/#dataflows
