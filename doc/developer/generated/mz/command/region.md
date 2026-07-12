---
source: src/mz/src/command/region.rs
revision: 2a40992526
---

# mz::command::region

Implements the `mz region` subcommand group using `RegionContext` to access the cloud API client.
`enable` creates or updates a cloud region, retrying up to 12 minutes total: first it retries `create_region` until it succeeds (up to 12 minutes), then retries polling `get_region` and checking SQL readiness (up to another 12 minutes).
`disable` deletes a region with retry up to 12 minutes; a 404 response is treated as success since the desired state already holds.
`list` displays all available cloud regions and their enabled/disabled status.
`show` retrieves and prints the health, SQL address, and HTTP URL of the active region.
