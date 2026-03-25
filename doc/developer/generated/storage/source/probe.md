---
source: src/storage/src/source/probe.rs
revision: 8dca3c638c
---

# mz-storage::source::probe

Provides `Ticker`, a time-interval scheduler used by source implementations to periodically probe the upstream system's frontier.
It rounds probe timestamps down to the nearest multiple of the tick interval to reduce time-series churn, and supports both async (`tick`) and blocking (`tick_blocking`) usage.
The tick interval is re-read from the provided closure after each tick, enabling dynamic reconfiguration.
If a tick is missed, it is skipped entirely rather than queued.
