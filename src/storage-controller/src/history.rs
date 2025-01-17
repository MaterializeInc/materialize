// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A reducible history of storage commands.

use std::collections::BTreeMap;

use mz_ore::cast::{CastFrom, CastInto};
use mz_storage_client::client::StorageCommand;
use mz_storage_client::metrics::HistoryMetrics;
use mz_storage_types::parameters::StorageParameters;

/// A history of storage commands.
#[derive(Debug)]
pub(crate) struct CommandHistory<T> {
    /// The number of commands at the last time we compacted the history.
    reduced_count: usize,
    /// The sequence of commands that should be applied.
    ///
    /// This list may not be "compact" in that there can be commands that could be optimized or
    /// removed given the context of other commands, for example compaction commands that can be
    /// unified, or run commands that can be dropped due to allowed compaction.
    commands: Vec<StorageCommand<T>>,
    /// Tracked metrics.
    metrics: HistoryMetrics,
}

impl<T: std::fmt::Debug> CommandHistory<T> {
    /// Constructs a new command history.
    pub fn new(metrics: HistoryMetrics) -> Self {
        metrics.reset();

        Self {
            reduced_count: 0,
            commands: Vec::new(),
            metrics,
        }
    }

    /// Returns an iterator over the contained storage commands.
    pub fn iter(&self) -> impl Iterator<Item = &StorageCommand<T>> {
        self.commands.iter()
    }

    /// Adds a command to the history.
    ///
    /// This action will reduce the history every time it doubles.
    pub fn push(&mut self, command: StorageCommand<T>) {
        use StorageCommand::*;

        self.commands.push(command);

        if self.commands.len() > 2 * self.reduced_count {
            self.reduce();
        } else {
            // Refresh reported metrics. `reduce` already refreshes metrics, so we only need to do
            // that here in the non-reduce case.
            let command = self.commands.last().expect("pushed above");
            let metrics = &self.metrics;
            match command {
                CreateTimely { .. } => metrics.create_timely_count.inc(),
                InitializationComplete => metrics.initialization_complete_count.inc(),
                AllowWrites => metrics.allow_writes_count.inc(),
                UpdateConfiguration(_) => metrics.update_configuration_count.inc(),
                RunIngestions(x) => metrics.run_ingestions_count.add(x.len().cast_into()),
                RunSinks(x) => metrics.run_sinks_count.add(x.len().cast_into()),
                AllowCompaction(x) => metrics.allow_compaction_count.add(x.len().cast_into()),
                RunOneshotIngestion(_) => {
                    // TODO(cf2): Add metrics for oneshot ingestions.
                }
            }
        }
    }

    /// Reduces the command history to a minimal form.
    pub fn reduce(&mut self) {
        use StorageCommand::*;

        let mut create_timely_command = None;
        let mut initialization_complete = false;
        let mut allow_writes = false;
        let mut final_compactions = BTreeMap::new();

        // Collect the final definitions of ingestions and sinks.
        // The same object ID can occur in multiple run commands when an object was altered. In
        // this scenario, we only want to send the most recent definition of the object.
        let mut final_ingestions = BTreeMap::new();
        let mut final_sinks = BTreeMap::new();
        let mut final_oneshot_ingestions = BTreeMap::new();

        // Collect only the final configuration.
        // Note that this means the final configuration is applied to all objects installed on the
        // new replica during initialization, even when the same objects where installed with an
        // older config on existing replicas. This is only correct as long as config parameters
        // don't affect the output of storage objects, as that would make different replicas write
        // different data, which is likely to produce inconsistencies.
        let mut final_configuration = StorageParameters::default();

        for command in self.commands.drain(..) {
            match command {
                cmd @ CreateTimely { .. } => create_timely_command = Some(cmd),
                InitializationComplete => initialization_complete = true,
                AllowWrites => allow_writes = true,
                UpdateConfiguration(params) => final_configuration.update(params),
                RunIngestions(cmds) => {
                    final_ingestions.extend(cmds.into_iter().map(|c| (c.id, c)));
                }
                RunSinks(cmds) => {
                    final_sinks.extend(cmds.into_iter().map(|c| (c.id, c)));
                }
                AllowCompaction(updates) => final_compactions.extend(updates),
                RunOneshotIngestion(oneshot) => {
                    final_oneshot_ingestions.insert(oneshot.ingestion_id, oneshot);
                }
            }
        }

        let mut run_ingestions = Vec::new();
        let mut run_sinks = Vec::new();
        let mut run_oneshot_ingestions = Vec::new();
        let mut allow_compaction = Vec::new();

        // Discard ingestions that have been dropped, keep the rest.
        for ingestion in final_ingestions.into_values() {
            if let Some(frontier) = final_compactions.get(&ingestion.id) {
                if frontier.is_empty() {
                    continue;
                }
            }

            let compactions = ingestion
                .description
                .collection_ids()
                .filter_map(|id| final_compactions.remove(&id).map(|f| (id, f)));
            allow_compaction.extend(compactions);

            run_ingestions.push(ingestion);
        }

        // Discard sinks that have been dropped, advance the as-of of the rest.
        for mut sink in final_sinks.into_values() {
            if let Some(frontier) = final_compactions.remove(&sink.id) {
                if frontier.is_empty() {
                    continue;
                }
                sink.description.as_of = frontier;
            }

            run_sinks.push(sink);
        }

        // TODO(cf1): Add a CancelOneshotIngestion command similar to CancelPeek
        // that will compact/reduce away the RunOneshotIngestion.
        run_oneshot_ingestions.extend(final_oneshot_ingestions.into_values());

        // Reconstitute the commands as a compact history.
        //
        // When we update `metrics`, we need to be careful to not transiently report incorrect
        // counts, as they would be observable by other threads. For example, we should not call
        // `metrics.reset()` here, since otherwise the command history would appear empty for a
        // brief amount of time.

        let count = u64::from(create_timely_command.is_some());
        self.metrics.create_timely_count.set(count);
        if let Some(create_timely_command) = create_timely_command {
            self.commands.push(create_timely_command);
        }

        let count = u64::from(!final_configuration.all_unset());
        self.metrics.update_configuration_count.set(count);
        if !final_configuration.all_unset() {
            self.commands
                .push(StorageCommand::UpdateConfiguration(final_configuration));
        }

        let count = u64::cast_from(run_ingestions.len());
        self.metrics.run_ingestions_count.set(count);
        if !run_ingestions.is_empty() {
            self.commands
                .push(StorageCommand::RunIngestions(run_ingestions));
        }

        let count = u64::cast_from(run_sinks.len());
        self.metrics.run_ingestions_count.set(count);
        if !run_sinks.is_empty() {
            self.commands.push(StorageCommand::RunSinks(run_sinks));
        }

        // TODO(cf1): Add a CancelOneshotIngestion command, make sure we prevent
        // re-sending commands for ingestions that we've already responded to.
        if !run_oneshot_ingestions.is_empty() {
            self.commands.extend(
                run_oneshot_ingestions
                    .into_iter()
                    .map(|oneshot| StorageCommand::RunOneshotIngestion(oneshot)),
            );
        }

        let count = u64::cast_from(allow_compaction.len());
        self.metrics.allow_compaction_count.set(count);
        if !allow_compaction.is_empty() {
            let updates = allow_compaction.into_iter().collect();
            self.commands.push(StorageCommand::AllowCompaction(updates));
        }

        let count = u64::from(initialization_complete);
        self.metrics.initialization_complete_count.set(count);
        if initialization_complete {
            self.commands.push(StorageCommand::InitializationComplete);
        }

        if allow_writes {
            self.commands.push(StorageCommand::AllowWrites);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use mz_cluster_client::metrics::ControllerMetrics;
    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::url::SensitiveUrl;
    use mz_persist_types::PersistLocation;
    use mz_repr::{CatalogItemId, GlobalId, RelationDesc, RelationType};
    use mz_storage_client::client::{RunIngestionCommand, RunSinkCommand};
    use mz_storage_client::metrics::StorageControllerMetrics;
    use mz_storage_types::connections::inline::InlinedConnection;
    use mz_storage_types::connections::{KafkaConnection, Tunnel};
    use mz_storage_types::controller::CollectionMetadata;
    use mz_storage_types::instances::StorageInstanceId;
    use mz_storage_types::sinks::{
        KafkaIdStyle, KafkaSinkCompressionType, KafkaSinkConnection, KafkaSinkFormat,
        KafkaSinkFormatType, MetadataFilled, SinkEnvelope, SinkPartitionStrategy,
        StorageSinkConnection, StorageSinkDesc,
    };
    use mz_storage_types::sources::load_generator::{
        LoadGenerator, LoadGeneratorOutput, LoadGeneratorSourceExportDetails,
    };
    use mz_storage_types::sources::{
        GenericSourceConnection, IngestionDescription, LoadGeneratorSourceConnection, SourceDesc,
        SourceEnvelope, SourceExport, SourceExportDataConfig, SourceExportDetails,
    };
    use timely::progress::Antichain;

    use super::*;

    fn history() -> CommandHistory<u64> {
        let registry = MetricsRegistry::new();
        let controller_metrics = ControllerMetrics::new(&registry);
        let metrics = StorageControllerMetrics::new(&registry, controller_metrics)
            .for_instance(StorageInstanceId::system(0).expect("0 is a valid ID"))
            .for_history();

        CommandHistory::new(metrics)
    }

    fn ingestion_description<S: Into<Vec<u64>>>(
        ingestion_id: u64,
        subsource_ids: S,
        remap_collection_id: u64,
    ) -> IngestionDescription<CollectionMetadata, InlinedConnection> {
        let export_ids = [ingestion_id, remap_collection_id]
            .into_iter()
            .chain(subsource_ids.into());
        let source_exports = export_ids
            .map(|id| {
                let export = SourceExport {
                    storage_metadata: CollectionMetadata {
                        persist_location: PersistLocation {
                            blob_uri: SensitiveUrl::from_str("mem://").expect("invalid URL"),
                            consensus_uri: SensitiveUrl::from_str("mem://").expect("invalid URL"),
                        },
                        remap_shard: Default::default(),
                        data_shard: Default::default(),
                        relation_desc: RelationDesc::new(
                            RelationType {
                                column_types: Default::default(),
                                keys: Default::default(),
                            },
                            Vec::<String>::new(),
                        ),
                        txns_shard: Default::default(),
                    },
                    details: SourceExportDetails::LoadGenerator(LoadGeneratorSourceExportDetails {
                        output: LoadGeneratorOutput::Default,
                    }),
                    data_config: SourceExportDataConfig {
                        encoding: Default::default(),
                        envelope: SourceEnvelope::CdcV2,
                    },
                };
                (GlobalId::User(id), export)
            })
            .collect();

        IngestionDescription {
            desc: SourceDesc {
                connection: GenericSourceConnection::LoadGenerator(LoadGeneratorSourceConnection {
                    load_generator: LoadGenerator::Auction,
                    tick_micros: Default::default(),
                    as_of: Default::default(),
                    up_to: Default::default(),
                }),
                primary_export: Some(SourceExportDataConfig {
                    encoding: Default::default(),
                    envelope: SourceEnvelope::CdcV2,
                }),
                timestamp_interval: Default::default(),
            },
            ingestion_metadata: CollectionMetadata {
                persist_location: PersistLocation {
                    blob_uri: SensitiveUrl::from_str("mem://").expect("invalid URL"),
                    consensus_uri: SensitiveUrl::from_str("mem://").expect("invalid URL"),
                },
                remap_shard: Default::default(),
                data_shard: Default::default(),
                relation_desc: RelationDesc::new(
                    RelationType {
                        column_types: Default::default(),
                        keys: Default::default(),
                    },
                    Vec::<String>::new(),
                ),
                txns_shard: Default::default(),
            },
            source_exports,
            instance_id: StorageInstanceId::system(0).expect("0 is a valid ID"),
            remap_collection_id: GlobalId::User(remap_collection_id),
        }
    }

    fn sink_description() -> StorageSinkDesc<MetadataFilled, u64> {
        StorageSinkDesc {
            from: GlobalId::System(1),
            from_desc: RelationDesc::new(
                RelationType {
                    column_types: Default::default(),
                    keys: Default::default(),
                },
                Vec::<String>::new(),
            ),
            connection: StorageSinkConnection::Kafka(KafkaSinkConnection {
                connection_id: CatalogItemId::System(2),
                connection: KafkaConnection {
                    brokers: Default::default(),
                    default_tunnel: Tunnel::Direct,
                    progress_topic: Default::default(),
                    progress_topic_options: Default::default(),
                    options: Default::default(),
                    tls: Default::default(),
                    sasl: Default::default(),
                },
                format: KafkaSinkFormat {
                    key_format: Default::default(),
                    value_format: KafkaSinkFormatType::Text,
                },
                relation_key_indices: Default::default(),
                key_desc_and_indices: Default::default(),
                headers_index: Default::default(),
                value_desc: RelationDesc::new(
                    RelationType {
                        column_types: Default::default(),
                        keys: Default::default(),
                    },
                    Vec::<String>::new(),
                ),
                partition_by: Default::default(),
                topic: Default::default(),
                topic_options: Default::default(),
                compression_type: KafkaSinkCompressionType::None,
                progress_group_id: KafkaIdStyle::Legacy,
                transactional_id: KafkaIdStyle::Legacy,
                topic_metadata_refresh_interval: Default::default(),
            }),
            partition_strategy: SinkPartitionStrategy::V1,
            with_snapshot: Default::default(),
            version: Default::default(),
            envelope: SinkEnvelope::Upsert,
            as_of: Antichain::from_elem(0),
            status_id: Default::default(),
            from_storage_metadata: CollectionMetadata {
                persist_location: PersistLocation {
                    blob_uri: SensitiveUrl::from_str("mem://").expect("invalid URL"),
                    consensus_uri: SensitiveUrl::from_str("mem://").expect("invalid URL"),
                },
                remap_shard: Default::default(),
                data_shard: Default::default(),
                relation_desc: RelationDesc::new(
                    RelationType {
                        column_types: Default::default(),
                        keys: Default::default(),
                    },
                    Vec::<String>::new(),
                ),
                txns_shard: Default::default(),
            },
        }
    }

    #[mz_ore::test]
    fn reduce_drops_dropped_ingestion() {
        let mut history = history();

        let commands = [
            StorageCommand::RunIngestions(vec![RunIngestionCommand {
                id: GlobalId::User(1),
                description: ingestion_description(1, [2], 3),
            }]),
            StorageCommand::AllowCompaction(vec![
                (GlobalId::User(1), Antichain::new()),
                (GlobalId::User(2), Antichain::new()),
                (GlobalId::User(3), Antichain::new()),
            ]),
        ];

        for cmd in commands {
            history.push(cmd);
        }

        history.reduce();

        let commands_after: Vec<_> = history.iter().collect();
        assert!(commands_after.is_empty(), "{:?}", commands_after);
    }

    #[mz_ore::test]
    fn reduce_keeps_compacted_ingestion() {
        let mut history = history();

        let commands = [
            StorageCommand::RunIngestions(vec![RunIngestionCommand {
                id: GlobalId::User(1),
                description: ingestion_description(1, [2], 3),
            }]),
            StorageCommand::AllowCompaction(vec![
                (GlobalId::User(1), Antichain::from_elem(1)),
                (GlobalId::User(2), Antichain::from_elem(2)),
                (GlobalId::User(3), Antichain::from_elem(3)),
            ]),
        ];

        for cmd in commands.clone() {
            history.push(cmd);
        }

        history.reduce();

        let commands_after: Vec<_> = history.iter().cloned().collect();
        assert_eq!(commands_after, commands);
    }

    #[mz_ore::test]
    fn reduce_keeps_partially_dropped_ingestion() {
        let mut history = history();

        let commands = [
            StorageCommand::RunIngestions(vec![RunIngestionCommand {
                id: GlobalId::User(1),
                description: ingestion_description(1, [2], 3),
            }]),
            StorageCommand::AllowCompaction(vec![(GlobalId::User(2), Antichain::new())]),
        ];

        for cmd in commands.clone() {
            history.push(cmd);
        }

        history.reduce();

        let commands_after: Vec<_> = history.iter().cloned().collect();
        assert_eq!(commands_after, commands);
    }

    #[mz_ore::test]
    fn reduce_drops_dropped_sink() {
        let mut history = history();

        let commands = [
            StorageCommand::RunSinks(vec![RunSinkCommand {
                id: GlobalId::User(1),
                description: sink_description(),
            }]),
            StorageCommand::AllowCompaction(vec![(GlobalId::User(1), Antichain::new())]),
        ];

        for cmd in commands {
            history.push(cmd);
        }

        history.reduce();

        let commands_after: Vec<_> = history.iter().collect();
        assert!(commands_after.is_empty(), "{:?}", commands_after);
    }

    #[mz_ore::test]
    fn reduce_keeps_compacted_sink() {
        let mut history = history();

        let sink_desc = sink_description();
        let commands = [
            StorageCommand::RunSinks(vec![RunSinkCommand {
                id: GlobalId::User(1),
                description: sink_desc.clone(),
            }]),
            StorageCommand::AllowCompaction(vec![(GlobalId::User(1), Antichain::from_elem(42))]),
        ];

        for cmd in commands {
            history.push(cmd);
        }

        history.reduce();

        let commands_after: Vec<_> = history.iter().cloned().collect();

        let expected_sink_desc = StorageSinkDesc {
            as_of: Antichain::from_elem(42),
            ..sink_desc
        };
        let expected_commands = [StorageCommand::RunSinks(vec![RunSinkCommand {
            id: GlobalId::User(1),
            description: expected_sink_desc,
        }])];

        assert_eq!(commands_after, expected_commands);
    }

    #[mz_ore::test]
    fn reduce_drops_stray_compactions() {
        let mut history = history();

        let commands = [
            StorageCommand::AllowCompaction(vec![(GlobalId::User(1), Antichain::new())]),
            StorageCommand::AllowCompaction(vec![
                (GlobalId::User(2), Antichain::from_elem(1)),
                (GlobalId::User(2), Antichain::from_elem(2)),
            ]),
        ];

        for cmd in commands {
            history.push(cmd);
        }

        history.reduce();

        let commands_after: Vec<_> = history.iter().collect();
        assert!(commands_after.is_empty(), "{:?}", commands_after);
    }
}
