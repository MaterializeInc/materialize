// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: a `SourceExportStatementDetails` must survive a proto encode +
//! decode round trip losslessly. The Rust side is a 5-variant enum
//! (Postgres / MySql / SqlServer / LoadGenerator / Kafka), so the conversion
//! has plenty of branches that need to round-trip. This value is serialized to
//! the catalog, so a decoder bug here is a corruption/migration risk.
//!
//! Two complementary input arms (the first byte picks):
//!
//!  * **Structured arm.** Synthesizes a *valid, populated* value. The
//!    Postgres / MySql / SqlServer variants carry a full table descriptor, which
//!    we generate with each `*TableDesc`'s proptest `Arbitrary` (driven from the
//!    libFuzzer byte stream). The load-generator output and the empty Kafka
//!    variant are picked directly. Random proto bytes essentially never produce
//!    a non-empty table descriptor, so this is where the nested column /
//!    constraint / key conversions actually get exercised.
//!  * **Raw-bytes arm.** Decodes arbitrary bytes straight into the proto,
//!    exercising the decoder against malformed/adversarial wire input (including
//!    the SQL Server `Lsn` `try_from` length guard, which is only reachable from
//!    raw bytes since a re-encoded `Lsn` is always exactly 10 bytes).
//!
//! `SourceExportStatementDetails` doesn't derive `PartialEq`/`Debug`, so
//! losslessness is asserted by comparing the canonical re-encoded bytes from
//! two successive `Rust -> Proto` round trips.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_mysql_util::MySqlTableDesc;
use mz_postgres_util::desc::PostgresTableDesc;
use mz_proto::ProtoType;
use mz_sql_server_util::desc::SqlServerTableDesc;
use mz_storage_types::sources::load_generator::LoadGeneratorOutput;
use mz_storage_types::sources::{ProtoSourceExportStatementDetails, SourceExportStatementDetails};
use prost::Message;
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};

/// Build a 32-byte proptest seed from `bytes` (zero-padded / truncated).
fn seed_from(bytes: &[u8]) -> [u8; 32] {
    let mut seed = [0u8; 32];
    let n = bytes.len().min(32);
    seed[..n].copy_from_slice(&bytes[..n]);
    seed
}

/// Generate one value of `T` via its proptest `Arbitrary`, or `None` if the
/// strategy fails.
fn arb<T: proptest::arbitrary::Arbitrary>(runner: &mut TestRunner) -> Option<T> {
    T::arbitrary().new_tree(runner).ok().map(|t| t.current())
}

/// The canonical proto encoding of `details`.
fn encode(details: &SourceExportStatementDetails) -> Vec<u8> {
    <ProtoSourceExportStatementDetails as ProtoType<SourceExportStatementDetails>>::from_rust(
        details,
    )
    .encode_to_vec()
}

/// `Rust -> Proto -> Rust -> Proto` must reproduce the same canonical bytes.
fn assert_roundtrip(orig: SourceExportStatementDetails) {
    let canonical = encode(&orig);
    let reparsed = ProtoSourceExportStatementDetails::decode(canonical.as_slice())
        .expect("re-encode of valid SourceExportStatementDetails must decode");
    let round: SourceExportStatementDetails = reparsed
        .into_rust()
        .expect("re-encoded SourceExportStatementDetails must convert back to Rust");
    assert_eq!(
        canonical,
        encode(&round),
        "SourceExportStatementDetails changed across proto roundtrip"
    );
}

fuzz_target!(|data: &[u8]| {
    let Some((&mode, rest)) = data.split_first() else {
        return;
    };

    if mode & 1 == 0 {
        // Structured arm: synthesize a valid value. Upper bits of `mode` select
        // which of the 5 variants to build.
        let seed = seed_from(rest);
        let mut runner = TestRunner::new_with_rng(
            Config::default(),
            TestRng::from_seed(RngAlgorithm::ChaCha, &seed),
        );
        let value = match (mode >> 1) % 5 {
            0 => {
                let Some(table) = arb::<PostgresTableDesc>(&mut runner) else {
                    return;
                };
                SourceExportStatementDetails::Postgres { table }
            }
            1 => {
                let Some(table) = arb::<MySqlTableDesc>(&mut runner) else {
                    return;
                };
                let Some(initial_gtid_set) = arb::<String>(&mut runner) else {
                    return;
                };
                let Some(binlog_full_metadata) = arb::<bool>(&mut runner) else {
                    return;
                };
                SourceExportStatementDetails::MySql {
                    table,
                    initial_gtid_set,
                    binlog_full_metadata,
                }
            }
            2 => {
                let Some(table) = arb::<SqlServerTableDesc>(&mut runner) else {
                    return;
                };
                let Some(capture_instance) = arb::<String>(&mut runner) else {
                    return;
                };
                let Some(initial_lsn) = arb::<mz_sql_server_util::cdc::Lsn>(&mut runner) else {
                    return;
                };
                SourceExportStatementDetails::SqlServer {
                    table,
                    capture_instance: capture_instance.into(),
                    initial_lsn,
                }
            }
            3 => {
                // Cover every `LoadGeneratorOutput` discriminant.
                let output = match rest.first().copied().unwrap_or(0) % 4 {
                    0 => LoadGeneratorOutput::Default,
                    1 => LoadGeneratorOutput::Auction(
                        mz_storage_types::sources::load_generator::AuctionView::Bids,
                    ),
                    2 => LoadGeneratorOutput::Marketing(
                        mz_storage_types::sources::load_generator::MarketingView::Leads,
                    ),
                    _ => LoadGeneratorOutput::Tpch(
                        mz_storage_types::sources::load_generator::TpchView::Customer,
                    ),
                };
                SourceExportStatementDetails::LoadGenerator { output }
            }
            _ => SourceExportStatementDetails::Kafka {},
        };
        assert_roundtrip(value);
    } else {
        // Raw-bytes arm: decode adversarial wire bytes, then round-trip.
        let Ok(proto) = ProtoSourceExportStatementDetails::decode(rest) else {
            return;
        };
        let orig: SourceExportStatementDetails = match proto.into_rust() {
            Ok(v) => v,
            Err(_) => return,
        };
        assert_roundtrip(orig);
    }
});
