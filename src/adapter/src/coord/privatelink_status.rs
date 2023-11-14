// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU32;
use std::{collections::BTreeMap, sync::Arc};

use futures::stream::StreamExt;
use futures::FutureExt;
use governor::{Quota, RateLimiter};

use mz_cloud_resources::VpcEndpointEvent;
use mz_ore::task::spawn;
use mz_repr::{Datum, GlobalId, Row};
use mz_storage_client::controller::IntrospectionType;

use crate::coord::Coordinator;

use super::Message;

impl Coordinator {
    pub(crate) fn spawn_privatelink_vpc_endpoints_watch_task(&self) {
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let rate_quota: i32 = self
            .catalog
            .system_config()
            .privatelink_status_update_quota_per_minute()
            .try_into()
            .expect("value coouldn't be converted to i32");
        if let Some(controller) = &self.cloud_resource_controller {
            let controller = Arc::clone(controller);
            spawn(|| "privatelink_vpc_endpoint_watch", async move {
                let mut stream = controller.watch_vpc_endpoints().await;
                // Using a per-minute quota implies a burst-size of the same amount
                let rate_limiter = RateLimiter::direct(Quota::per_minute(
                    NonZeroU32::new(rate_quota).expect("will be non-zero"),
                ));

                loop {
                    // Wait for an event to become available
                    if let Some(event) = stream.next().await {
                        // Wait until we're permitted to tell the coordinator about the event
                        rate_limiter.until_ready().await;

                        // Drain any additional events from the queue that accumulated while
                        // waiting for the rate limiter, deduplicating by connection_id.
                        let mut events = BTreeMap::new();
                        events.insert(event.connection_id, event);
                        while let Some(event) = stream.next().now_or_never().and_then(|e| e) {
                            events.insert(event.connection_id, event);
                        }

                        // Send the event batch to the coordinator.
                        let _ = internal_cmd_tx.send(Message::PrivateLinkVpcEndpointEvents(events));
                    }
                }
            });
        }
    }

    pub(crate) async fn write_privatelink_status_updates(
        &mut self,
        events: BTreeMap<GlobalId, VpcEndpointEvent>,
    ) {
        let mut updates = Vec::new();
        for value in events.into_values() {
            updates.push((
                Row::pack_slice(&[
                    Datum::TimestampTz(value.time.try_into().expect("must fit")),
                    Datum::String(&value.connection_id.to_string()),
                    Datum::String(&value.status.to_string()),
                ]),
                1,
            ));
        }

        self.controller
            .storage
            .record_introspection_updates(
                IntrospectionType::PrivatelinkConnectionStatusHistory,
                updates,
            )
            .await;
    }
}
