// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use k8s_controller::events::Reporter;

pub mod balancer;
pub mod console;
pub mod materialize;

/// The reporter that the events published by the controller named
/// `controller` (one of the `CONTROLLER_NAME`s) carry. `instance` identifies
/// this replica.
pub fn event_reporter(controller: &str, instance: String) -> Reporter {
    Reporter {
        controller: format!("orchestratord.materialize.cloud/{controller}"),
        instance: Some(instance),
    }
}
