// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to source caching.
//
// TODO: currently everything is fairly Kafka-centric and we should probably
// not directly usable for some other source types.

use std::path::{Path, PathBuf};

use anyhow::Error;
use log::error;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use expr::{GlobalId, PartitionId};
use repr::CachedRecord;

use crate::source::ConsistencyInfo;

static RECORD_FILE_PREFIX: &str = "materialize";

/// Describes what is provided from a cached file.
#[derive(Debug)]
pub struct RecordFileMetadata {
    /// The cluster id of the Materialize instance that wrote this file.
    pub cluster_id: Uuid,
    /// The source global ID this file represents.
    pub source_id: GlobalId,
    /// The partition ID this file represents.
    pub partition_id: i32,
    /// The inclusive lower bound of offsets provided by this file.
    pub start_offset: i64,
    /// The exclusive upper bound of offsets provided by this file.
    pub end_offset: i64,
}

impl RecordFileMetadata {
    /// Parse a file's metadata from its path.
    pub fn from_path(path: &Path) -> Result<Option<Self>, Error> {
        let file_name = path.file_name();

        if file_name.is_none() {
            // Path ends in .. . This should never happen but let's
            // just ignore for now.
            return Ok(None);
        }

        let file_name = file_name.expect("known to have a file name").to_str();

        if file_name.is_none() {
            // Path cannot be converted to a UTF-8 string. This
            // should not be the case for cache files as we
            // control every aspect of the name.
            // TODO(rkhaitan): Make sure this assumption is valid.
            return Ok(None);
        }

        let file_name = file_name.expect("known to be a valid UTF-8 file name");

        if !file_name.starts_with(RECORD_FILE_PREFIX) {
            // File name doesn't match the prefix we use to write
            // down cache data.
            return Ok(None);
        }

        let parts: Vec<_> = file_name.split('-').collect();

        if parts.len() != 6 {
            // File is either partially written, or entirely irrelevant.
            error!("Found invalid cache file name: {}. Ignoring", file_name);
            return Ok(None);
        }
        Ok(Some(Self {
            cluster_id: Uuid::parse_str(parts[1])?,
            source_id: parts[2].parse()?,
            partition_id: parts[3].parse()?,
            // Here we revert the transformation we made to convert this to a 0-indexed
            // offset in `generate_file_name`.
            start_offset: parts[4].parse::<i64>()? + 1,
            end_offset: parts[5].parse()?,
        }))
    }

    /// Generate a file name that can later be parsed into metadata.
    pub fn generate_file_name(
        cluster_id: Uuid,
        source_id: GlobalId,
        partition_id: i32,
        start_offset: i64,
        end_offset: i64,
    ) -> String {
        // We get start and end offsets as 1-indexed MzOffsets that denote the set of
        // offsets [start, end] (in 1-indexed offsets). Unfortunately, Kafka offsets are
        // actually 0-indexed, and therefore this construction is not easily explainable to
        // users. We will instead convert this to [start, end) in 0-indexed offsets.
        // TODO(rkhaitan): revisit MzOffsets being 1-indexed. This seems extremely confusing
        // for questionable value.
        assert!(
            start_offset > 0,
            "start offset has to be a valid 1-indexed offset"
        );
        format!(
            "{}-{}-{}-{}-{}-{}",
            RECORD_FILE_PREFIX,
            cluster_id.to_simple(),
            source_id,
            partition_id,
            start_offset - 1,
            end_offset
        )
    }
}

/// Source data that gets sent to the cache thread to flush to the cache.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkerCacheData {
    /// Global Id of the Source whose data is being cached.
    pub source_id: GlobalId,
    /// Partition the record belongs to.
    pub partition_id: i32,
    /// The record itself.
    pub record: CachedRecord,
}

/// Get partition id information from a path to cached data.
pub fn cached_file_partition(path: &Path) -> Option<PartitionId> {
    match RecordFileMetadata::from_path(path) {
        Ok(Some(meta)) => {
            // Currently source caching only supports Kafka
            Some(PartitionId::Kafka(meta.partition_id))
        }
        _ => {
            error!(
                "failed to get cache metadata info about path: {}",
                path.display()
            );
            None
        }
    }
}

/// Extract the subset of cached data files a given worker needs to replay.
///
/// Note that this list is ordered in reverse offset order, so that users can pop()
/// elements in offset order.
pub fn cached_files_for_worker(
    source_id: GlobalId,
    files: Vec<PathBuf>,
    consistency_info: &ConsistencyInfo,
) -> Vec<PathBuf> {
    let mut cached_files: Vec<_> = files
        .iter()
        .map(|f| {
            let metadata = RecordFileMetadata::from_path(f);
            (f, metadata)
        })
        .filter(|(f, metadata)| {
            // We partition the given partitions up amongst workers, so we need to be
            // careful not to process a partition that this worker was not allocated (or
            // else we would process files multiple times).
            match metadata {
                Ok(Some(meta)) => {
                    assert_eq!(source_id, meta.source_id);
                    // Source caching is only enabled for Kafka sources at the moment
                    consistency_info.responsible_for(&PartitionId::Kafka(meta.partition_id))
                }
                _ => {
                    error!("Failed to parse path: {}", f.display());
                    false
                }
            }
        })
        .collect::<Vec<_>>();
    cached_files.sort_by_key(|(_, metadata)| match metadata {
        Ok(Some(meta)) => -meta.start_offset,
        _ => unreachable!(),
    });

    cached_files.iter().map(|(f, _)| (*f).clone()).collect()
}
