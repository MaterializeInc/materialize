// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The process-global settings a compute runtime applies from its configuration.
//!
//! lgalloc, the memory limiter, the columnation lgalloc region, the overflowing behavior, and
//! arrangement dictionary compression each have one value per process. A process that runs two
//! compute runtimes applies them from one runtime only: the other would double-apply effects that
//! are not idempotent, or race the first.

use std::path::PathBuf;

use mz_compute_types::dyncfgs::{
    ENABLE_COLUMNATION_LGALLOC, ENABLE_LGALLOC, ENABLE_LGALLOC_EAGER_RECLAMATION,
    LGALLOC_BACKGROUND_INTERVAL, LGALLOC_FILE_GROWTH_DAMPENER, LGALLOC_LOCAL_BUFFER_BYTES,
    LGALLOC_SLOW_CLEAR_BYTES,
};
use mz_dyncfg::ConfigSet;
use mz_storage_types::dyncfgs::ORE_OVERFLOWING_BEHAVIOR;
use tracing::{debug, error, info};

/// Whether a runtime applies the process-global settings or inherits them. Chosen once, when the
/// runtime is built.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ProcessGlobals {
    /// The runtime applies the settings.
    Apply,
    /// Another runtime in the process applies the settings, and this one inherits them.
    Inherit,
}

impl ProcessGlobals {
    /// Applies the settings `config` carries.
    pub(crate) fn apply_config(self, config: &ConfigSet, scratch_directory: Option<&PathBuf>) {
        match self {
            ProcessGlobals::Inherit => {}
            ProcessGlobals::Apply => {
                apply_lgalloc(config, scratch_directory);
                crate::memory_limiter::apply_limiter_config(config);
                mz_ore::region::ENABLE_LGALLOC_REGION.store(
                    ENABLE_COLUMNATION_LGALLOC.get(config),
                    std::sync::atomic::Ordering::Relaxed,
                );
                let overflowing_behavior = ORE_OVERFLOWING_BEHAVIOR.get(config);
                match overflowing_behavior.parse() {
                    Ok(behavior) => mz_ore::overflowing::set_behavior(behavior),
                    Err(err) => {
                        error!(
                            err,
                            overflowing_behavior, "Invalid value for ore_overflowing_behavior"
                        );
                    }
                }
            }
        }
    }

    /// Sets arrangement dictionary compression, which is captured once per replica.
    pub(crate) fn apply_dictionary_compression(self, enabled: bool) {
        match self {
            ProcessGlobals::Inherit => {}
            ProcessGlobals::Apply => mz_row_spine::DICTIONARY_COMPRESSION
                .store(enabled, std::sync::atomic::Ordering::Relaxed),
        }
    }
}

fn apply_lgalloc(config: &ConfigSet, scratch_directory: Option<&PathBuf>) {
    if !ENABLE_LGALLOC.get(config) {
        info!("disabling lgalloc");
        lgalloc::lgalloc_set_config(lgalloc::LgAlloc::new().disable());
        return;
    }
    let Some(path) = scratch_directory else {
        debug!("not enabling lgalloc, scratch directory not specified");
        return;
    };
    let clear_bytes = LGALLOC_SLOW_CLEAR_BYTES.get(config);
    let eager_return = ENABLE_LGALLOC_EAGER_RECLAMATION.get(config);
    let file_growth_dampener = LGALLOC_FILE_GROWTH_DAMPENER.get(config);
    let interval = LGALLOC_BACKGROUND_INTERVAL.get(config);
    let local_buffer_bytes = LGALLOC_LOCAL_BUFFER_BYTES.get(config);
    info!(
        ?path,
        backgrund_interval=?interval,
        clear_bytes,
        eager_return,
        file_growth_dampener,
        local_buffer_bytes,
        "enabling lgalloc"
    );
    let background_worker_config = lgalloc::BackgroundWorkerConfig {
        interval,
        clear_bytes,
    };
    lgalloc::lgalloc_set_config(
        lgalloc::LgAlloc::new()
            .enable()
            .with_path(path.clone())
            .with_background_config(background_worker_config)
            .eager_return(eager_return)
            .file_growth_dampener(file_growth_dampener)
            .local_buffer_bytes(local_buffer_bytes),
    );
}
