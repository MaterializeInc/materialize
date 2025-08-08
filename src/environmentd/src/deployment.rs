// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Environment deployments.
//!
//! A deployment is a graceful transition of an environment to a new version or
//! configuration [^1]. This module provides the components that facilitate
//! these deployments.
//!
//! Deployments are initiated and driven externally to `environmentd`. At a very
//! high level, the process works like this:
//!
//!   1. The external orchestrator starts a new `environmentd` process with a
//!      deploy generation that is greater than the previous deploy generation
//!      (e.g., if the current deploy generation is 1, the new `environmentd`
//!      process should be started with `--deploy-generation=2`).
//!
//!   2. The new `environmentd` process initializes to the extent possible. At
//!      the moment the initialization process is quite weak and only validates
//!      that the catalog is loadable. We plan to strengthen the initialization
//!      process over time to include starting all `clusterd` processes in the
//!      new deployment and ensuring that they rehydrate successfully.
//!
//!   3. The new deployment announces that it is ready to take over from the
//!      previous generation (`ReadyToPromote`) via the internal HTTP server.
//!
//!   4. The external orchestrator promotes the new deployment via the HTTP
//!      server.
//!
//!   5. The new deployment completes any remaining initialization tasks and
//!      prepares to serve queries.
//!
//!   6. Simultaneously, the external orchestrator tears down the old
//!      deployment.
//!
//! [^1]: "Configuration" here means "command-line flags". Most settings in an
//! environment are changeable at runtime via LaunchDarkly feature flags, but
//! occasionally we have settings that are only changeable via command-line
//! flags to `environmentd`. Changing these flags requires a process restart,
//! and performing that process restart without incurring unavailability
//! requires this deployment infrastructure.

pub mod preflight;
pub mod state;
