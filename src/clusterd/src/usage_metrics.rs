// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Support for collecting system usage metrics.
//!
//! Currently only disk usage is supported. We may want to add CPU and memory usage in the future.

use std::path::PathBuf;

use serde::Serialize;
use tracing::error;

/// A system usage metrics collector.
pub(crate) struct Collector {
    pub disk_root: Option<PathBuf>,
}

impl Collector {
    /// Collect current system usage metrics.
    pub fn collect(&self) -> Usage {
        Usage {
            disk_bytes: self.collect_disk_usage(),
        }
    }

    fn collect_disk_usage(&self) -> Option<u64> {
        let Some(root) = &self.disk_root else {
            return None;
        };

        let stat = match nix::sys::statvfs::statvfs(root) {
            Ok(stat) => stat,
            Err(err) => {
                error!("statvfs error: {err}");
                return None;
            }
        };

        // `fsblkcnt_t` is a `u32` on macOS but a `u64` on Linux.
        #[allow(clippy::useless_conversion)]
        let used_blocks = u64::from(stat.blocks() - stat.blocks_available());
        let used_bytes = used_blocks * stat.fragment_size();

        Some(used_bytes)
    }
}

/// A system usage measurement.
#[derive(Serialize)]
pub(crate) struct Usage {
    disk_bytes: Option<u64>,
}
