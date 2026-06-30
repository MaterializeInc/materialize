// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: catalog object serde JSON round-trip is idempotent. The
//! catalog state is durable on-disk data, so a serde edge case that loses
//! information through a JSON round trip is a catalog-corruption risk.
//!
//! Two complementary input arms (the first byte picks the arm, the next byte
//! picks which catalog type to exercise):
//!
//!  * **Structured arm.** Drives the catalog type's proptest `Arbitrary`
//!    (behind mz-catalog-protos' `proptest` feature) from the libFuzzer byte
//!    stream to synthesize a *valid, deeply-populated* value, then asserts the
//!    full `value -> JSON -> value -> JSON` chain is idempotent. We deliberately
//!    target the genuinely nested catalog types: `ClusterValue`
//!    (`RoleId` + `Vec<MzAclItem>` + the `ClusterConfig`/`ClusterVariant`/
//!    `ManagedCluster`/`ClusterSchedule` tree), `ItemValue` (the `CatalogItem`
//!    enum + `GlobalId` enum + `Vec<ItemVersion>`), `RoleValue` (the
//!    `RoleAttributes`/`RoleMembership`/`RoleVars`/`RoleVar` tree),
//!    `NetworkPolicyValue` (`Vec<NetworkPolicyRule>`), and `ClusterReplicaValue`
//!    (the `ReplicaConfig`/`ReplicaLocation` enum). Random JSON bytes almost
//!    never reach these inner enum variants, so this is where the interesting
//!    serde branches actually get covered.
//!  * **Raw-bytes arm.** Deserializes arbitrary bytes straight into the type,
//!    exercising the deserializer against malformed/adversarial JSON input,
//!    then re-serializes the recovered value.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_catalog_protos::objects::{
    ClusterConfig, ClusterReplicaValue, ClusterValue, ConfigValue, GidMappingValue, ItemValue,
    MzAclItem, NetworkPolicyValue, RoleId, RoleValue, SettingValue,
};
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};

/// Build a 32-byte proptest seed from `bytes` (zero-padded / truncated).
fn seed_from(bytes: &[u8]) -> [u8; 32] {
    let mut seed = [0u8; 32];
    let n = bytes.len().min(32);
    seed[..n].copy_from_slice(&bytes[..n]);
    seed
}

/// Synthesize a valid `T` via its proptest `Arbitrary`, then assert the serde
/// JSON round trip is idempotent.
fn structured_roundtrip<T>(seed: &[u8])
where
    T: serde::de::DeserializeOwned
        + serde::Serialize
        + PartialEq
        + std::fmt::Debug
        + proptest::arbitrary::Arbitrary,
{
    let mut runner = TestRunner::new_with_rng(
        Config::default(),
        TestRng::from_seed(RngAlgorithm::ChaCha, &seed_from(seed)),
    );
    let Ok(tree) = T::arbitrary().new_tree(&mut runner) else {
        return;
    };
    assert_idempotent(tree.current());
}

/// `value -> JSON -> value` must be the identity, and re-serializing must
/// produce byte-identical JSON.
fn assert_idempotent<T>(orig: T)
where
    T: serde::de::DeserializeOwned + serde::Serialize + PartialEq + std::fmt::Debug,
{
    let json = serde_json::to_vec(&orig).expect("serialize of valid value must succeed");
    let round: T = serde_json::from_slice(&json).expect("re-decode must round-trip");
    assert_eq!(orig, round, "serde roundtrip changed value");
    let json2 = serde_json::to_vec(&round).expect("re-serialize must succeed");
    assert_eq!(json, json2, "serde re-serialize was not idempotent");
}

/// Decode adversarial JSON bytes straight into `T`, then assert the recovered
/// value round-trips.
fn raw_roundtrip<T>(data: &[u8])
where
    T: serde::de::DeserializeOwned + serde::Serialize + PartialEq + std::fmt::Debug,
{
    let Ok(orig) = serde_json::from_slice::<T>(data) else {
        return;
    };
    assert_idempotent(orig);
}

fuzz_target!(|data: &[u8]| {
    let Some((&mode, rest)) = data.split_first() else {
        return;
    };
    let Some((&which, rest)) = rest.split_first() else {
        return;
    };

    if mode & 1 == 0 {
        // Structured arm: synthesize a valid, deeply-nested value.
        match which % 10 {
            0 => structured_roundtrip::<ClusterValue>(rest),
            1 => structured_roundtrip::<ItemValue>(rest),
            2 => structured_roundtrip::<RoleValue>(rest),
            3 => structured_roundtrip::<NetworkPolicyValue>(rest),
            4 => structured_roundtrip::<ClusterReplicaValue>(rest),
            5 => structured_roundtrip::<GidMappingValue>(rest),
            6 => structured_roundtrip::<ClusterConfig>(rest),
            7 => structured_roundtrip::<MzAclItem>(rest),
            8 => structured_roundtrip::<RoleId>(rest),
            _ => structured_roundtrip::<ConfigValue>(rest),
        }
    } else {
        // Raw-bytes arm: decode adversarial JSON, then round-trip.
        match which % 10 {
            0 => raw_roundtrip::<ClusterValue>(rest),
            1 => raw_roundtrip::<ItemValue>(rest),
            2 => raw_roundtrip::<RoleValue>(rest),
            3 => raw_roundtrip::<NetworkPolicyValue>(rest),
            4 => raw_roundtrip::<ClusterReplicaValue>(rest),
            5 => raw_roundtrip::<GidMappingValue>(rest),
            6 => raw_roundtrip::<ConfigValue>(rest),
            7 => raw_roundtrip::<SettingValue>(rest),
            8 => raw_roundtrip::<MzAclItem>(rest),
            _ => raw_roundtrip::<RoleId>(rest),
        }
    }
});
