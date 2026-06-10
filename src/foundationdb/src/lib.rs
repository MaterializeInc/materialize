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
use mz_ore::url::SensitiveUrl;

/// Re-export the `foundationdb` crate for convenience.
pub use foundationdb::*;

/// FoundationDB network handle.
/// The first element is `Some` if the network is initialized.
/// The second element is `true` if the network has ever been initialized.
static FDB_NETWORK: Mutex<(Option<NetworkAutoStop>, bool)> = Mutex::new((None, false));

/// Initialize the FoundationDB network.
///
/// This function is safe to call multiple times - only the first call will
/// actually initialize the network, subsequent calls return immediately.
///
/// After calling `shutdown_network()`, any subsequent calls to this function
/// will panic.
///
/// The user is required to call [`shutdown_network()`] before the process exits to
/// ensure a clean shutdown of the FoundationDB network. Otherwise, strange memory
/// corruption issues during shutdown may occur. This is a limitation of the
/// FoundationDB C API.
pub fn init_network() {
    let mut guard = FDB_NETWORK.lock().expect("mutex poisoned");
    if guard.0.is_none() {
        if guard.1 {
            panic!("attempted to re-initialize FoundationDB network after shutdown");
        }
        // SAFETY: The `foundationdb::boot()` call is unsafe because it must only
        // be called once per process. We use a mutex to ensure this guarantee
        // is upheld - subsequent calls to `init_network()` will see `guard.is_some()`
        // and return early without calling `boot()` again.
        guard.0 = Some(unsafe { boot() });
        guard.1 = true;
    }
}

/// Shut down the FoundationDB network.
///
/// After calling this function, any subsequent calls to `init_network()` will panic.
pub fn shutdown_network() {
    let mut guard = FDB_NETWORK.lock().expect("mutex poisoned");
    if guard.0.is_some() {
        guard.0 = None;
    }
}

/// Configuration parsed from a FoundationDB URL.
///
/// FoundationDB URLs have the format:
/// `foundationdb:?prefix=<prefix>`
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
    /// The URL format is: `foundationdb:?prefix=<prefix>`
    ///
    /// - The scheme must be `foundationdb`
    /// - The `prefix` query parameter specifies the directory prefix to use,
    ///   with path components separated by `/`
    ///
    /// The cluster file is NOT specified in the URL. Instead, FoundationDB's
    /// standard discovery mechanism is used (via `FDB_CLUSTER_FILE` env var
    /// or the default `/etc/foundationdb/fdb.cluster`).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Use default cluster file with a prefix
    /// let url = "foundationdb:?prefix=my_app/consensus";
    /// ```
    pub fn parse(url: &SensitiveUrl) -> Result<Self, anyhow::Error> {
        let mut prefix = None;

        let mut legacy_prefix = None;

        for (key, value) in url.query_pairs() {
            match &*key {
                "prefix" => {
                    prefix = Some(value.split('/').map(|s| s.to_owned()).collect());
                }
                "options" => {
                    tracing::warn!(
                        "FoundationDB URL 'options' parameter is deprecated; use 'prefix' instead"
                    );
                    // Parse a string like `--search_path=<path>` to extract legacy prefix.
                    if let Some(stripped) = value.strip_prefix("--search_path=") {
                        legacy_prefix = Some(stripped.split('/').map(|s| s.to_owned()).collect());
                    } else {
                        anyhow::bail!("unrecognized FoundationDB URL options parameter: {value}");
                    }
                }
                key => {
                    anyhow::bail!("unrecognized FoundationDB URL query parameter: {key}={value}");
                }
            }
        }

        if prefix.is_some() && legacy_prefix.is_some() {
            anyhow::bail!(
                "cannot specify both 'prefix' and legacy 'options' parameters in FoundationDB URL"
            );
        }

        Ok(FdbConfig {
            prefix: prefix.or(legacy_prefix).unwrap_or_default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::str::FromStr;

    #[mz_ore::test]
    fn test_parse_url_with_prefix() {
        let url = SensitiveUrl::from_str("foundationdb:?prefix=my_app/consensus").unwrap();
        let config = FdbConfig::parse(&url).unwrap();
        assert_eq!(config.prefix, vec!["my_app", "consensus"]);
    }

    #[mz_ore::test]
    fn test_parse_url_with_nested_prefix() {
        let url = SensitiveUrl::from_str("foundationdb:?prefix=a/b/c/d").unwrap();
        let config = FdbConfig::parse(&url).unwrap();
        assert_eq!(config.prefix, vec!["a", "b", "c", "d"]);
    }

    #[mz_ore::test]
    fn test_parse_url_no_prefix() {
        let url = SensitiveUrl::from_str("foundationdb:").unwrap();
        let config = FdbConfig::parse(&url).unwrap();
        assert!(config.prefix.is_empty());
    }

    #[mz_ore::test]
    fn test_parse_url_invalid_query_param() {
        let url = SensitiveUrl::from_str("foundationdb:?unknown=value").unwrap();
        assert!(FdbConfig::parse(&url).is_err());
    }
}
