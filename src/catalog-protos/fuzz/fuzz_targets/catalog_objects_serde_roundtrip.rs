// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: catalog object serde round-trips are lossless. The catalog
//! state is durable on-disk data, so an encoding edge case that loses
//! information is a catalog-corruption risk.
//!
//! The durable form is not JSON *text*. `StateUpdateKindJson` packs the serde
//! value into a `Jsonb`, i.e. an `mz_repr::Row`, and reads it back out through
//! `to_serde_json` + `from_value`. That JSONB leg is the one that can lose
//! information: every number becomes a `Datum::Numeric` and object keys are
//! deduplicated and reordered by the `Row` map encoding. So the oracle asserts
//! that leg, not just `to_vec`/`from_slice`, which never touches it.
//!
//! Two complementary input arms (the first byte picks the arm, the next byte
//! picks which catalog type to exercise). Both arms dispatch over the same type
//! list in the same order, so a corpus entry's type byte means the same thing in
//! either arm:
//!
//!  * **Structured arm.** Drives the catalog type's proptest `Arbitrary`
//!    (behind mz-catalog-protos' `proptest` feature) from the libFuzzer byte
//!    stream to synthesize a *valid, deeply-populated* value, then asserts both
//!    round trips. The list leads with `StateUpdateKind`, the durable envelope
//!    every catalog write goes through and the one member of this family whose
//!    round trip is not trivially total: it is `#[serde(tag = "kind")]`, so it
//!    deserializes through serde's content-buffering path rather than straight
//!    from the input. The rest are the genuinely nested values reached through
//!    it: `ClusterValue` (`RoleId` + `Vec<MzAclItem>` + the
//!    `ClusterConfig`/`ClusterVariant`/`ManagedCluster`/`ClusterSchedule` tree),
//!    `ItemValue` (the `CatalogItem` enum + `GlobalId` enum +
//!    `Vec<ItemVersion>`), `RoleValue` (the
//!    `RoleAttributes`/`RoleMembership`/`RoleVars`/`RoleVar` tree),
//!    `NetworkPolicyValue` (`Vec<NetworkPolicyRule>`), and `ClusterReplicaValue`
//!    (the `ReplicaConfig`/`ReplicaLocation` enum). Random JSON bytes almost
//!    never reach these inner enum variants, so this is where the interesting
//!    serde branches actually get covered.
//!  * **Raw-bytes arm.** Deserializes arbitrary bytes straight into the type,
//!    exercising the deserializer against malformed/adversarial JSON input,
//!    then round-trips the recovered value. The nested types need a complete
//!    object with the right field names and variant tags to get past the decode,
//!    which is what `catalog_objects_serde_roundtrip.dict` supplies.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_catalog_protos::objects::{
    ClusterConfig, ClusterReplicaValue, ClusterValue, ConfigValue, GidMappingValue, ItemValue,
    MzAclItem, NetworkPolicyValue, RoleId, RoleValue, SettingValue, StateUpdateKind,
};
use mz_repr::adt::jsonb::Jsonb;
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

/// `value -> encoded -> value` must be the identity, through both the JSON text
/// form and the JSONB form the durable catalog actually stores.
fn assert_idempotent<T>(orig: T)
where
    T: serde::de::DeserializeOwned + serde::Serialize + PartialEq + std::fmt::Debug,
{
    // JSON text: what a `to_vec`/`from_slice` consumer sees.
    let json = serde_json::to_vec(&orig).expect("serialize of valid value must succeed");
    let round: T = serde_json::from_slice(&json).expect("re-decode must round-trip");
    assert_eq!(orig, round, "serde roundtrip changed value");

    // The durable path, `StateUpdateKindJson::from_serde` / `try_to_serde`: pack
    // the serde value into a `Row` and read it back. Numbers survive as
    // `Datum::Numeric` only because every integer in these types fits in
    // `Numeric`'s 39 digits (`u64::MAX` is 20) and `to_standard_notation_string`
    // never emits exponent notation, which is a property worth asserting rather
    // than assuming.
    let value = serde_json::to_value(&orig).expect("serialize to a value must succeed");
    let jsonb = Jsonb::from_serde_json(value).expect("catalog value must pack as jsonb");
    let via_jsonb: T = serde_json::from_value(jsonb.as_ref().to_serde_json())
        .expect("jsonb must round-trip back into the catalog type");
    assert_eq!(orig, via_jsonb, "jsonb roundtrip changed value");
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

/// Dispatch `$arm` over the catalog types. One list for both arms, so the type
/// byte selects the same type either way and a corpus entry stays meaningful
/// across a one-bit change to the arm byte.
macro_rules! dispatch {
    ($which:expr, $arm:ident, $rest:expr) => {
        match $which % 12 {
            0 => $arm::<StateUpdateKind>($rest),
            1 => $arm::<ClusterValue>($rest),
            2 => $arm::<ItemValue>($rest),
            3 => $arm::<RoleValue>($rest),
            4 => $arm::<NetworkPolicyValue>($rest),
            5 => $arm::<ClusterReplicaValue>($rest),
            6 => $arm::<ClusterConfig>($rest),
            7 => $arm::<GidMappingValue>($rest),
            8 => $arm::<MzAclItem>($rest),
            9 => $arm::<RoleId>($rest),
            10 => $arm::<ConfigValue>($rest),
            _ => $arm::<SettingValue>($rest),
        }
    };
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
        dispatch!(which, structured_roundtrip, rest)
    } else {
        // Raw-bytes arm: decode adversarial JSON, then round-trip.
        dispatch!(which, raw_roundtrip, rest)
    }
});
