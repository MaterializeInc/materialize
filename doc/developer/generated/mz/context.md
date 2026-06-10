---
source: src/mz/src/context.rs
revision: f38003ddc8
---

# mz::context

Defines three context types representing different levels of CLI authentication state: `Context` (config file + output formatter), `ProfileContext` (adds admin, cloud, and SQL clients from an activated profile), and `RegionContext` (adds a resolved region name and provides cloud-provider lookup).
Commands take exactly the context level they require; `Context::activate_profile` and `ProfileContext::activate_region` perform the stepwise initialization.
