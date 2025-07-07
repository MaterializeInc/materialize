Replicating tables that contain unsupported data types is possible via:

- the [`TEXT
  COLUMNS`](/sql/create-table/#tab-source-populated-tables-via-db-connector)
  option for the following unsupported types:

  - `enum`
  - `year`

  The specified columns will be treated as `text` and will not offer the
original MySQL type features.

- the [`EXCLUDE
  COLUMNS`](/sql/create-table/#tab-source-populated-tables-via-db-connector)
  option for all unsupported data types.
