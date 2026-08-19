// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `RelationDesc` proto round-trips losslessly.
//! `RelationDesc` is the schema of every persisted collection, so a decoder
//! bug here corrupts catalog/persist state.
//!
//! Three arms (the first byte selects):
//!  - Arbitrary arm: drive `RelationDesc`'s proptest `Arbitrary` strategy from
//!    the fuzzer bytes, then apply a generated batch of schema migrations to
//!    it. The strategy alone only ever builds descs through `RelationDesc::new`,
//!    whose metadata is all-default, so a dropped column, and with it the
//!    encoder's non-default metadata path, enters the value space only via the
//!    migrations. Assert the desc survives an encode/decode through the wire
//!    codec. Valid values are where a proto3 presence bug shows up: a
//!    default-valued field that silently stops being written to the wire still
//!    passes an in-memory `RustType` round-trip.
//!  - Versioned arm: build a desc through `VersionedRelationDesc`'s
//!    `add_column`/`drop_column` and read it back `at_version`, which is the
//!    only producer of *sparse* `ColumnIndex` keys. The proto has no field for
//!    those keys, so the round trip renumbers them densely. Assert what does
//!    survive, and pin the renumbering itself, since
//!    `ColumnIndex::to_stable_name` is the arrow field name persist data is
//!    written under.
//!  - Raw-bytes arm: decode arbitrary bytes as `ProtoRelationDesc`, into Rust,
//!    and re-encode, keeping coverage of the bare wire decoder against hostile
//!    input. Equality across the re-encode is weak here, `from_proto`
//!    canonicalizes what it decodes so it holds for anything that decodes at
//!    all, so the arm also walks the decoded desc: the decoder must not admit
//!    a shape that panics on first use.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_proto::{ProtoType, protobuf_roundtrip};
use mz_repr::{
    ColumnName, ProtoRelationDesc, RelationDesc, RelationVersion, RelationVersionSelector,
    SqlColumnType, VersionedRelationDesc, arb_relation_desc_diff,
};
use proptest::prelude::any;
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};
use prost::Message;

fn runner(seed: &[u8]) -> TestRunner {
    let mut buf = [0u8; 32];
    for (dst, src) in buf.iter_mut().zip(seed.iter()) {
        *dst = *src;
    }
    // NOTE: hashing the fuzzer bytes into a ChaCha seed costs libFuzzer its
    // mutation gradient, one flipped bit re-rolls the whole value. The obvious
    // fix, `RngAlgorithm::PassThrough`, hangs: it feeds the fuzzer bytes in as
    // the random stream and then yields zeros forever once they run out, and
    // `rand`'s Lemire sampler loops until a draw clears `thresh`, which a zero
    // never does for a range that is not a power of two. Every strategy here
    // outdraws a 4096-byte input.
    let rng = TestRng::from_seed(RngAlgorithm::ChaCha, &buf);
    TestRunner::new_with_rng(Config::default(), rng)
}

fn arbitrary_arm(seed: &[u8]) {
    let mut runner = runner(seed);
    let strat = any::<RelationDesc>().prop_flat_map(|desc| {
        arb_relation_desc_diff(&desc).prop_map(move |diffs| (desc.clone(), diffs))
    });
    let Ok(tree) = strat.new_tree(&mut runner) else {
        return;
    };
    let (mut value, diffs) = tree.current();
    for diff in diffs {
        diff.apply(&mut value);
    }

    let back = protobuf_roundtrip::<_, ProtoRelationDesc>(&value)
        .expect("valid RelationDesc must round-trip");
    assert_eq!(value, back, "RelationDesc changed across proto roundtrip");
}

fn versioned_arm(seed: &[u8]) {
    let mut runner = runner(seed);
    let strat = (
        any::<RelationDesc>(),
        proptest::collection::vec((any::<bool>(), any::<u8>(), any::<SqlColumnType>()), 0..8),
        any::<u8>(),
    );
    let Ok(tree) = strat.new_tree(&mut runner) else {
        return;
    };
    let (desc, ops, version) = tree.current();

    let mut versioned = VersionedRelationDesc::new(desc);
    for (idx, (drop, pick, typ)) in ops.into_iter().enumerate() {
        let live = versioned.at_version(RelationVersionSelector::Latest);
        // `drop_column` panics on a column that is part of a key. `arb_relation_desc` doesn't
        // generate keys, this only keeps the arm honest if that changes.
        let droppable: Vec<ColumnName> = if live.typ().keys.is_empty() {
            live.iter_names().cloned().collect()
        } else {
            Vec::new()
        };
        if drop && !droppable.is_empty() {
            let name = droppable[usize::from(pick) % droppable.len()].clone();
            let _ = versioned.drop_column(name);
        } else {
            // `add_column` panics on a name that is already live.
            let name = ColumnName::from(format!("fuzz_added_{idx}"));
            if live.get_by_name(&name).is_none() {
                let _ = versioned.add_column(name, typ);
            }
        }
    }

    let value = versioned.at_version(RelationVersionSelector::Specific(
        RelationVersion::from_raw(u64::from(version)),
    ));
    let back = protobuf_roundtrip::<_, ProtoRelationDesc>(&value)
        .expect("valid RelationDesc must round-trip");

    // The column data must survive in `ColumnIndex` order, as must the keys.
    assert_eq!(
        value.arity(),
        back.arity(),
        "arity changed: {value:?} {back:?}"
    );
    assert!(
        value.iter().eq(back.iter()),
        "columns changed across proto roundtrip: {value:?} {back:?}"
    );
    assert_eq!(
        value.typ().keys,
        back.typ().keys,
        "keys changed across proto roundtrip: {value:?} {back:?}"
    );

    // The `ColumnIndex` keys themselves do not survive: the proto carries only the values, in
    // index order, so decoding renumbers them to `0..n`. Pin that, both to catch indexes
    // shifting some other way and so that teaching the proto to carry them has to come back
    // through here and through the `to_stable_name` consumers.
    assert!(
        back.iter_all()
            .enumerate()
            .all(|(pos, (idx, _, _))| idx.to_raw() == pos),
        "decoded ColumnIndexes are not dense: {back:?}"
    );
    let was_dense = value
        .iter_all()
        .enumerate()
        .all(|(pos, (idx, _, _))| idx.to_raw() == pos);
    if was_dense {
        assert_eq!(
            value, back,
            "dense RelationDesc changed across proto roundtrip"
        );
    }

    // Whatever the indexes were, the renumbered desc is a fixed point.
    let again = protobuf_roundtrip::<_, ProtoRelationDesc>(&back)
        .expect("re-encoded RelationDesc must round-trip");
    assert_eq!(back, again, "RelationDesc roundtrip is not idempotent");
}

fn raw_arm(data: &[u8]) {
    let Ok(proto) = ProtoRelationDesc::decode(data) else {
        return;
    };
    let orig: RelationDesc = match proto.into_rust() {
        Ok(v) => v,
        Err(_) => return,
    };

    // A desc that decodes must be *usable*. `iter()` indexes `typ.columns()` by `typ_idx` and
    // `into_iter()` zips the metadata against `column_types` with `zip_eq`, so a shape the
    // decoder lets through panics at first use, inside whichever timely worker touches the
    // collection, rather than at the boundary. Walking it here turns that into a fuzz finding.
    assert_eq!(orig.iter().count(), orig.arity());
    assert_eq!(orig.clone().into_iter().count(), orig.arity());
    for key in orig.typ().keys.iter().flatten() {
        assert!(
            *key < orig.arity(),
            "key {key} out of bounds for {} columns",
            orig.arity()
        );
    }

    let proto2 = <ProtoRelationDesc as ProtoType<RelationDesc>>::from_rust(&orig);
    let bytes2 = proto2.encode_to_vec();
    let proto3 = ProtoRelationDesc::decode(bytes2.as_slice())
        .expect("re-encode of valid RelationDesc must decode");
    let round: RelationDesc = proto3
        .into_rust()
        .expect("re-encoded RelationDesc must convert back to Rust");

    assert_eq!(orig, round, "RelationDesc changed across proto roundtrip");
}

fuzz_target!(|data: &[u8]| {
    let Some((&mode, rest)) = data.split_first() else {
        return;
    };
    match mode % 3 {
        0 => arbitrary_arm(rest),
        1 => versioned_arm(rest),
        _ => raw_arm(rest),
    }
});
