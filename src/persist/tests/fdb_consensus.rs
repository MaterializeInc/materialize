// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![cfg(any(target_os = "linux", feature = "fdb"))]

//! Integration test for the FoundationDB consensus implementation.
//!
//! This test is in a separate file to ensure it runs as a separate test binary,
//! since the FDB network can only be initialized once per process and must be
//! properly shut down to avoid crashes during process exit.

use bytes::Bytes;
use mz_persist::foundationdb::{FdbConsensus, FdbConsensusConfig};
use mz_persist::location::tests::consensus_impl_test;
use mz_persist::location::{CaSResult, Consensus, ExternalError, SeqNo, VersionedData};
use uuid::Uuid;

/// Drops and recreates the `consensus` data in FoundationDB.
///
/// ONLY FOR TESTING
async fn drop_and_recreate(consensus: &FdbConsensus) -> Result<(), ExternalError> {
    use foundationdb::directory::Directory;

    consensus
        .db
        .run(async |trx, _maybe_commited| {
            consensus.seqno.remove(&trx, &[]).await?;
            consensus.data.remove(&trx, &[]).await?;
            Ok(())
        })
        .await?;
    Ok(())
}

#[mz_ore::test(tokio::test(flavor = "multi_thread"))]
#[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn fdb_consensus() -> Result<(), ExternalError> {
    let config = FdbConsensusConfig::new(
        std::str::FromStr::from_str("foundationdb:?options=--search_path=test/consensus").unwrap(),
    )?;

    {
        let fdb = FdbConsensus::open(config.clone()).await?;
        drop_and_recreate(&fdb).await?;
    }

    consensus_impl_test(|| FdbConsensus::open(config.clone())).await?;

    // and now verify the implementation-specific `drop_and_recreate` works as intended
    let consensus = FdbConsensus::open(config.clone()).await?;
    let key = Uuid::new_v4().to_string();
    let mut state = VersionedData {
        seqno: SeqNo(5),
        data: Bytes::from("abc"),
    };

    assert_eq!(
        consensus.compare_and_set(&key, None, state.clone()).await,
        Ok(CaSResult::Committed),
    );
    state.seqno = SeqNo(6);
    assert_eq!(
        consensus
            .compare_and_set(&key, Some(SeqNo(5)), state.clone())
            .await,
        Ok(CaSResult::Committed),
    );
    state.seqno = SeqNo(129 + 5);
    assert_eq!(
        consensus
            .compare_and_set(&key, Some(SeqNo(6)), state.clone())
            .await,
        Ok(CaSResult::Committed),
    );

    assert_eq!(consensus.head(&key).await, Ok(Some(state.clone())));

    println!("--- SCANNING ---");

    for data in consensus.scan(&key, SeqNo(129), 10).await? {
        println!(
            "scan data: seqno: {:?}, {} bytes",
            data.seqno,
            data.data.len()
        );
    }

    drop_and_recreate(&consensus).await?;

    assert_eq!(consensus.head(&key).await, Ok(None));

    // Properly shut down the FDB network at the end of the test.
    mz_foundationdb::shutdown_network();

    Ok(())
}
