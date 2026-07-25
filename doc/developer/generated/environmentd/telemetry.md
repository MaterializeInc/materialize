---
source: src/environmentd/src/telemetry.rs
revision: 4e2b6c0984
---

# environmentd::telemetry

Runs a periodic reporting loop that queries the adapter for environment statistics (active sources, sinks, views, clusters, etc.) and sends them to Segment via `group` (latest state) and `track` ("Environment Rolled Up" event with delta statistics).
The loop starts via `start_reporting` and fires at a configurable `report_interval`; the first tick only sends traits, subsequent ticks also compute and report delta counters for DML operations.
`Config` carries a `license_key: ValidatedLicenseKey` field. Each reporting interval, the loop merges the license key's organization ID, environment ID, key ID, expiration timestamp, expired flag, and expiration behavior into the traits map before the Segment `group` and `track` calls. Expiry is recomputed against the wall clock each interval so a key that lapses while environmentd keeps running is reported as expired.
