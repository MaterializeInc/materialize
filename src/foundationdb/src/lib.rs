// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Common FoundationDB utilities for Materialize.
//!
//! This crate provides shared functionality for FoundationDB-backed
//! implementations across Materialize, including network initialization
//! and URL parsing utilities.

use std::sync::Mutex;

use foundationdb::api::NetworkAutoStop;
use foundationdb::options::NetworkOption;
use mz_ore::url::SensitiveUrl;

/// FoundationDB network handle, stored in a Mutex for proper lifecycle management.
static FDB_NETWORK: Mutex<Option<NetworkAutoStop>> = Mutex::new(None);

/// Initialize the FoundationDB network.
///
/// This function is safe to call multiple times - only the first call will
/// actually initialize the network, subsequent calls return immediately.
///
/// # Safety
///
/// The underlying `foundationdb::boot()` call is unsafe because it must only
/// be called once per process. This function uses a mutex to ensure
/// that guarantee is upheld.
pub fn init_network() {
    let mut guard = FDB_NETWORK.lock().expect("FDB_NETWORK mutex poisoned");
    if guard.is_none() {
        let network = unsafe { foundationdb::boot() };

        // Disable client statistics logging to reduce metrics overhead.
        let _ = unsafe { NetworkOption::DisableClientStatisticsLogging.apply() };

        *guard = Some(network);
    }
}

/// Shut down the FoundationDB network.
///
/// This properly stops the network thread and cleans up resources.
/// Should be called at the end of tests that use FoundationDB.
///
/// # Warning
///
/// FDB can only be initialized once per process. After calling this function,
/// any subsequent calls to `init_network()` will fail.
pub fn shutdown_network() {
    if let Ok(mut guard) = FDB_NETWORK.lock() {
        // Dropping the NetworkAutoStop calls fdb_stop_network() and joins the thread.
        drop(guard.take());
    }
}

/// Configuration parsed from a FoundationDB URL.
///
/// FoundationDB URLs have the format:
/// `foundationdb:?options=--search_path=<prefix>`
///
/// The cluster file is determined by FoundationDB's standard discovery mechanism:
/// 1. The `FDB_CLUSTER_FILE` environment variable
/// 2. The default path `/etc/foundationdb/fdb.cluster`
///
/// This ensures all components using FoundationDB connect to the same cluster.
#[derive(Clone, Debug)]
pub struct FdbConfig {
    /// The prefix path components for the directory layer.
    pub prefix: Vec<String>,
}

impl FdbConfig {
    /// Parse a FoundationDB URL into configuration.
    ///
    /// # URL Format
    ///
    /// The URL format is: `foundationdb:?options=--search_path=<prefix>`
    ///
    /// - The scheme must be `foundationdb`
    /// - The `options` query parameter with `--search_path=<prefix>` specifies
    ///   the directory prefix to use (similar to PostgreSQL's search_path)
    ///
    /// The cluster file is NOT specified in the URL. Instead, FoundationDB's
    /// standard discovery mechanism is used (via `FDB_CLUSTER_FILE` env var
    /// or the default `/etc/foundationdb/fdb.cluster`).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Use default cluster file with a prefix
    /// let url = "foundationdb:?options=--search_path=my_app/consensus";
    /// ```
    pub fn parse(url: &SensitiveUrl) -> Result<Self, anyhow::Error> {
        let mut prefix = Vec::new();

        for (key, value) in url.query_pairs() {
            match &*key {
                "options" => {
                    if let Some(path) = value.strip_prefix("--search_path=") {
                        prefix = path.split('/').map(|s| s.to_owned()).collect();
                    } else {
                        anyhow::bail!("unrecognized FoundationDB URL options parameter: {value}");
                    }
                }
                key => {
                    anyhow::bail!("unrecognized FoundationDB URL query parameter: {key}: {value}");
                }
            }
        }

        Ok(FdbConfig { prefix })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::str::FromStr;

    #[mz_ore::test]
    fn test_parse_url_with_prefix() {
        let url =
            SensitiveUrl::from_str("foundationdb:?options=--search_path=my_app/consensus").unwrap();
        let config = FdbConfig::parse(&url).unwrap();
        assert_eq!(config.prefix, vec!["my_app", "consensus"]);
    }

    #[mz_ore::test]
    fn test_parse_url_with_nested_prefix() {
        let url = SensitiveUrl::from_str("foundationdb:?options=--search_path=a/b/c/d").unwrap();
        let config = FdbConfig::parse(&url).unwrap();
        assert_eq!(config.prefix, vec!["a", "b", "c", "d"]);
    }

    #[mz_ore::test]
    fn test_parse_url_no_options() {
        let url = SensitiveUrl::from_str("foundationdb:").unwrap();
        let config = FdbConfig::parse(&url).unwrap();
        assert!(config.prefix.is_empty());
    }

    #[mz_ore::test]
    fn test_parse_url_invalid_option() {
        let url = SensitiveUrl::from_str("foundationdb:?options=--invalid=value").unwrap();
        assert!(FdbConfig::parse(&url).is_err());
    }

    #[mz_ore::test]
    fn test_parse_url_invalid_query_param() {
        let url = SensitiveUrl::from_str("foundationdb:?unknown=value").unwrap();
        assert!(FdbConfig::parse(&url).is_err());
    }
}
