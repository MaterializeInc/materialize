---
source: src/mz/src/command/region.rs
revision: c10148c42f
---

# mz::command::region

Implements the `mz region` subcommand: `enable` creates or updates a cloud region (with retry up to 12 minutes), and `list` displays all available regions and their status.
Uses `RegionContext` to access the cloud API client.
