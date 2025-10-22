The `time`, `datetime2`, and `datetimeoffset` types in SQL Server have a default
scale of 7 decimal places, or in other words a accuracy of 100 nanoseconds. But
the corresponding types in Materialize only support a scale of 6 decimal places.
If a column in SQL Server has a higher scale than what Materialize can support, it
will be rounded up to the largest scale possible.

```
-- In SQL Server
CREATE TABLE my_timestamps (a datetime2(7));
INSERT INTO my_timestamps VALUES
  ('2000-12-31 23:59:59.99999'),
  ('2000-12-31 23:59:59.999999'),
  ('2000-12-31 23:59:59.9999999');

-- Replicated into Materialize
SELECT * FROM my_timestamps;
'2000-12-31 23:59:59.999990'
'2000-12-31 23:59:59.999999'
'2001-01-01 00:00:00'
