// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU32;
use std::sync::Arc;

use governor::{Quota, RateLimiter};

use mz_ore::future::OreStreamExt;
use mz_ore::task::spawn;

use crate::coord::Coordinator;

use super::Message;

impl Coordinator {
    pub(crate) fn spawn_privatelink_vpc_endpoints_watch_task(&self) {
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let rate_quota: u32 = self
            .catalog
            .system_config()
            .privatelink_status_update_quota_per_minute();

        if let Some(controller) = &self.cloud_resource_controller {
            let controller = Arc::clone(controller);
            spawn(|| "privatelink_vpc_endpoint_watch", async move {
                let mut stream = controller.watch_vpc_endpoints().await;
                // Using a per-minute quota implies a burst-size of the same amount
                let rate_limiter = RateLimiter::direct(Quota::per_minute(
                    NonZeroU32::new(rate_quota).expect("will be non-zero"),
                ));

                loop {
                    // Wait for events to become available
                    if let Some(new_events) = stream.recv_many(20).await {
                        // Wait until we're permitted to tell the coordinator about the events
                        // Note that the stream is backed by a https://docs.rs/kube/latest/kube/runtime/fn.watcher.html,
                        // which means its safe for us to rate limit for an arbitrarily long time and expect the stream
                        // to continue to work, despite not being polled
                        rate_limiter.until_ready().await;

                        // Send the event batch to the coordinator to be written
                        let _ =
                            internal_cmd_tx.send(Message::PrivateLinkVpcEndpointEvents(new_events));
                    }
                }
            });
        }
    }
}
