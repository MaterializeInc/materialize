---
headless: true
---

Any `ORDER BY` applied before the aggregate function is evaluated, such as in
a feeding subquery, is ignored. Unless `ORDER BY` is included in the aggregate
function call itself, the order in which the values are aggregated is
unspecified.
