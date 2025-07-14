`mz_now()` must be used with one of the following comparison operators: `=`,
`<`, `<=`, `>`, `>=`, or an operator that desugars to them or to a conjunction
(`AND`) of them (for example, `BETWEEN...AND...`). That is, you cannot use
date/time operations directly on  `mz_now()` to calculate timestamp in the past
or future. Instead, rewrite the query expression to have the  move the operation
to the other side of the comparison.
