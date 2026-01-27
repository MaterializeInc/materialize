// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![cfg(any(target_os = "linux", feature = "fdb"))]

//! Integration test for the FoundationDB timestamp oracle.
//!
//! This test is in a separate file to ensure it runs as a separate test binary,
//! since the FDB network can only be initialized once per process and must be
//! properly shut down to avoid crashes during process exit.

use std::sync::Arc;

use mz_ore::now::NowFn;
use mz_repr::Timestamp;
use mz_timestamp_oracle::TimestampOracle;
use mz_timestamp_oracle::foundationdb_oracle::{FdbTimestampOracle, FdbTimestampOracleConfig};

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn test_fdb_timestamp_oracle() -> Result<(), anyhow::Error> {
    let config = FdbTimestampOracleConfig::new_for_test();

    mz_timestamp_oracle::tests::timestamp_oracle_impl_test(
        |timeline, now_fn: NowFn, initial_ts| {
            let config = config.clone();
            async move {
                let oracle = FdbTimestampOracle::open(config, timeline, initial_ts, now_fn, false)
                    .await
                    .expect("failed to open FdbTimestampOracle");

                let arced_oracle: Arc<dyn TimestampOracle<Timestamp> + Send + Sync> =
                    Arc::new(oracle);

                arced_oracle
            }
        },
    )
    .await?;

    // Properly shut down the FDB network at the end of the test.
    mz_foundationdb::shutdown_network();

    Ok(())
}
