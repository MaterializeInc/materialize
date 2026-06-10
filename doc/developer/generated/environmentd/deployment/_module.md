---
source: src/environmentd/src/deployment.rs
revision: 844ad57e4b
---

# environmentd::deployment

Provides the infrastructure for zero-downtime (0dt) environment deployments.
The module tracks deployment state through a `DeploymentState` machine (`state`) and orchestrates the preflight check sequence (`preflight`) that determines whether a new `environmentd` generation should boot read-only and wait for promotion or proceed directly as the leader.
