---
headless: true
---
Materialize natively supports the following SQL Server types:

{{< multicolumn-list columns="3" >}}
- `tinyint`
- `smallint`
- `int`
- `bigint`
- `real`
- `double precision`
- `float`
- `bit`
- `decimal`
- `numeric`
- `money`
- `smallmoney`
- `char`
- `nchar`
- `varchar`
- `varchar(max)`
- `nvarchar`
- `nvarchar(max)`
- `sysname`
- `binary`
- `varbinary`
- `json`
- `date`
- `time`
- `smalldatetime`
- `datetime`
- `datetime2`
- `datetimeoffset`
- `uniqueidentifier`
{{</ multicolumn-list >}}

#### `char` and `nchar` columns

To preserve values exactly as SQL Server returns them, `char` and `nchar` columns
are replicated as `text` rather than fixed-length. SQL Server and Materialize
measure fixed-length character types differently, so replicating as text avoids
truncation and padding mismatches.
