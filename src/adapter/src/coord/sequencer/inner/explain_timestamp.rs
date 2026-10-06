// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use itertools::Itertools;
use mz_adapter_types::connection::ConnectionId;
use mz_controller_types::ClusterId;

use crate::coord::Coordinator;
use crate::coord::timestamp_selection::{TimestampDetermination, TimestampSource};
use crate::{CollectionIdBundle, TimestampExplanation};

impl Coordinator {
    pub(crate) fn explain_timestamp(
        &self,
        conn_id: &ConnectionId,
        session_wall_time: DateTime<Utc>,
        cluster_id: ClusterId,
        id_bundle: &CollectionIdBundle,
        determination: TimestampDetermination,
    ) -> TimestampExplanation {
        let mut sources = Vec::new();
        {
            let storage_ids = id_bundle.storage_ids.iter().cloned().collect_vec();
            let frontiers = self
                .controller
                .storage
                .collections_frontiers(storage_ids)
                .expect("missing collection");

            for (id, since, upper) in frontiers {
                let name = self
                    .catalog()
                    .try_get_entry_by_global_id(&id)
                    .map(|item| item.name())
                    .map(|name| {
                        self.catalog()
                            .resolve_full_name(name, Some(conn_id))
                            .to_string()
                    })
                    .unwrap_or_else(|| id.to_string());
                sources.push(TimestampSource {
                    name: format!("{name} ({id}, storage)"),
                    read_frontier: since.elements().to_vec(),
                    write_frontier: upper.elements().to_vec(),
                });
            }
        }
        {
            if let Some(compute_ids) = id_bundle.compute_ids.get(&cluster_id) {
                let catalog = self.catalog();
                for id in compute_ids {
                    let frontiers = self
                        .controller
                        .compute
                        .collection_frontiers(*id, Some(cluster_id))
                        .expect("id does not exist");
                    let name = catalog
                        .try_get_entry_by_global_id(id)
                        .map(|item| item.name())
                        .map(|name| catalog.resolve_full_name(name, Some(conn_id)).to_string())
                        .unwrap_or_else(|| id.to_string());
                    sources.push(TimestampSource {
                        name: format!("{name} ({id}, compute)"),
                        read_frontier: frontiers.read_frontier.to_vec(),
                        write_frontier: frontiers.write_frontier.to_vec(),
                    });
                }
            }
        }
        let respond_immediately = determination.respond_immediately();
        TimestampExplanation {
            determination,
            sources,
            session_wall_time,
            respond_immediately,
        }
    }
}
