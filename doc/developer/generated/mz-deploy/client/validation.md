---
source: src/mz-deploy/src/client/validation.rs
revision: d8759e6fea
---

# mz-deploy::client::validation

Database validation operations.
System-catalog dependencies (those with no database component, such as `mz_catalog` schema objects) are excluded from external-dependency existence checks because they are always present and their 2-part name never matches the 3-part FQN the existence query builds.
