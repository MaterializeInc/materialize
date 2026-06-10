---
source: src/environmentd/src/deployment/state.rs
revision: 6ec833a937
---

# environmentd::deployment::state

Defines `DeploymentState` and `DeploymentStateHandle`, a shared state machine that tracks a deployment through its lifecycle: `Initializing → CatchingUp → ReadyToPromote → Promoting → IsLeader`.
`DeploymentState` is held by the server and drives transitions (e.g. `set_catching_up`, `set_ready_to_promote`, `set_is_leader`), while `DeploymentStateHandle` is given to external interfaces such as the HTTP server to query the current status and trigger promotion.
