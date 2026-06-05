---
source: src/repr/src/refresh_schedule.rs
revision: 2d7058372f
---

# mz-repr::refresh_schedule

Defines `RefreshSchedule`, which encodes the `REFRESH EVERY` / `REFRESH AT` options for materialized views, providing methods to compute the next refresh time and to check whether a given timestamp falls on a scheduled boundary.
