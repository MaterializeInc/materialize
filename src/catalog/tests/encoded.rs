// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(unknown_lints)]
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![allow(clippy::drain_collect)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
// END LINT CONFIG

use std::collections::BTreeSet;
use std::io::Write;
use std::str::FromStr;
use std::{fs, iter};

use mz_catalog::durable::StateUpdateKindRaw;
use mz_persist_types::Codec;
use mz_storage_types::sources::SourceData;
use proptest::prelude::*;
use proptest::strategy::ValueTree;

const ENCODED_TEST_CASES: usize = 100;

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_proto_serialization_stability() {
    let proto_directory = format!("{}/protos", env!("CARGO_MANIFEST_DIR"));
    let protos: BTreeSet<_> = read_file_names(&proto_directory, "proto")
        // Remove `objects.proto`.
        //
        // `objects.proto` is allowed to change and we don't have a good
        // mechanism to force people to update `objects.txt` any time `objects.proto` changes. To
        // avoid this rot we just don't test the contents of `objects.proto`. Additionally,
        // `objects.proto` will always be identical to the most recent `objects_vX.proto`.
        .filter(|name| name != "objects")
        .collect();

    let encoded_directory = format!("{}/tests/encoded", env!("CARGO_MANIFEST_DIR"));
    let encoded_files: BTreeSet<_> = read_file_names(&encoded_directory, "txt").collect();

    let unknown_encoded: Vec<_> = encoded_files.difference(&protos).collect();
    if !unknown_encoded.is_empty() {
        panic!("Have encoded objects, but no proto files on disk? If a .proto file was deleted, then the .txt encoded file must be deleted too. {unknown_encoded:#?}");
    }

    let unencoded_protos: Vec<_> = protos.difference(&encoded_files).collect();
    if !unencoded_protos.is_empty() {
        panic!("Missing encodings for some proto objects, try generating them with `generate_missing_encodings`. {unencoded_protos:#?}");
    }

    let base64_config = base64::Config::new(base64::CharacterSet::Standard, true);
    for encoded_file in encoded_files {
        let encoded_bytes = fs::read(format!("{encoded_directory}/{encoded_file}.txt"))
            .expect("unable to read encoded file");
        let encoded_str = std::str::from_utf8(encoded_bytes.as_slice()).expect("valid UTF-8");
        let proto_version = ProtoVersion::from_str(&encoded_file).unwrap();
        let decoded: Vec<_> = encoded_str
            .lines()
            .map(|s| base64::decode_config(s, base64_config).expect("valid base64"))
            .map(|b| SourceData::decode(&b).expect("valid proto"))
            .map(StateUpdateKindRaw::from)
            .map(|raw| proto_version.roundtrip(raw))
            .map(SourceData::from)
            .collect();

        // Reencode and compare the strings.
        let mut reencoded = String::new();
        let mut buf = vec![];
        for source_data in decoded {
            buf.clear();
            source_data.encode(&mut buf);
            base64::encode_config_buf(buf.as_slice(), base64_config, &mut reencoded);
            reencoded.push('\n');
        }

        // Consolidation in the catalog depends on stable serialization for SourceData.
        assert_eq!(
            encoded_str,
            reencoded.as_str(),
            "SourceData serde should be stable"
        )
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // not an actual test
#[ignore]
/// This is not a real test, it is a helper to generate encoded catalog objects for other tests.
/// When you want to generate new encodings, then uncomment `#[ignore]` and run this test.
fn generate_missing_encodings() {
    let proto_directory = format!("{}/protos", env!("CARGO_MANIFEST_DIR"));
    let protos: BTreeSet<_> = read_file_names(&proto_directory, "proto")
        .filter(|name| name != "objects")
        .collect();

    let encoded_directory = format!("{}/tests/encoded", env!("CARGO_MANIFEST_DIR"));
    let encoded: BTreeSet<_> = read_file_names(&encoded_directory, "txt").collect();

    let unknown_encoded: Vec<_> = encoded.difference(&protos).collect();
    if !unknown_encoded.is_empty() {
        panic!("Have encoded objects, but no proto files on disk? {unknown_encoded:#?}");
    }

    let base64_config = base64::Config::new(base64::CharacterSet::Standard, true);

    for to_encode in protos.difference(&encoded) {
        let mut file = fs::File::options()
            .create_new(true)
            .write(true)
            .open(format!("{encoded_directory}/{to_encode}.txt"))
            .unwrap();
        let raw_datas = ProtoVersion::from_str(to_encode).unwrap().arbitrary_raws();
        for raw_data in raw_datas {
            let source_data: SourceData = raw_data.into();
            let mut buf = Vec::new();
            source_data.encode(&mut buf);
            let mut encoded = String::new();
            base64::encode_config_buf(buf.as_slice(), base64_config, &mut encoded);

            write!(&mut file, "{encoded}\n").expect("unable to write file");
        }
    }
}

fn read_file_names<'a>(dir: &'a str, ext: &'a str) -> impl Iterator<Item = String> + 'a {
    fs::read_dir(dir)
        .unwrap()
        // If we fail to read one file, fail everything.
        .collect::<Result<Vec<_>, _>>()
        .expect("unable to read directory")
        .into_iter()
        // Filter to only files with the .`ext` extension.
        .filter(move |entry| {
            entry
                .path()
                .extension()
                .map(|e| e.to_string_lossy().contains(ext))
                .unwrap_or(false)
        })
        // Remove file extension.
        .map(|entry| {
            entry
                .path()
                .file_stem()
                .expect("no filename")
                .to_str()
                .expect("UTF-8")
                .to_string()
        })
}

enum ProtoVersion {
    V42,
    V43,
    V44,
    V45,
}

impl FromStr for ProtoVersion {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "objects_v42" => Ok(Self::V42),
            "objects_v43" => Ok(Self::V43),
            "objects_v44" => Ok(Self::V44),
            "objects_v45" => Ok(Self::V45),

            _ => Err(format!("unrecognized version {s} add enum variant")),
        }
    }
}

impl ProtoVersion {
    /// Generate a vec of random [`StateUpdateKindRaw`].
    fn arbitrary_raws(&self) -> Vec<StateUpdateKindRaw> {
        let mut runner = proptest::test_runner::TestRunner::deterministic();
        iter::repeat(())
            .filter_map(|_| self.arbitrary_raw(&mut runner))
            .take(ENCODED_TEST_CASES)
            .collect()
    }

    fn arbitrary_raw(
        &self,
        runner: &mut proptest::test_runner::TestRunner,
    ) -> Option<StateUpdateKindRaw> {
        macro_rules! raw_data {
            ($strategy:expr, $runner:expr) => {{
                let arbitrary_data = $strategy
                    .new_tree($runner)
                    .expect("unable to create arbitrary data")
                    .current();
                // Skip over generated data where kind is None because they are not interesting or
                // possible in production. Unfortunately any of the inner fields can still be None,
                // which is also not possible in production.
                // TODO(jkosh44) See if there's an arbitrary config that forces Some.
                if arbitrary_data.kind.is_some() {
                    let raw_data: StateUpdateKindRaw = arbitrary_data.into();
                    Some(raw_data)
                } else {
                    None
                }
            }};
        }

        match self {
            ProtoVersion::V42 => raw_data!(
                mz_catalog::durable::upgrade::objects_v42::StateUpdateKind::arbitrary(),
                runner
            ),
            ProtoVersion::V43 => raw_data!(
                mz_catalog::durable::upgrade::objects_v43::StateUpdateKind::arbitrary(),
                runner
            ),
            ProtoVersion::V44 => raw_data!(
                mz_catalog::durable::upgrade::objects_v44::StateUpdateKind::arbitrary(),
                runner
            ),
            ProtoVersion::V45 => raw_data!(
                mz_catalog::durable::upgrade::objects_v45::StateUpdateKind::arbitrary(),
                runner
            ),
        }
    }

    /// Roundtrip [`StateUpdateKindRaw`] through a specific versioned protobuf `StateUpdateKind`.
    fn roundtrip(&self, raw: StateUpdateKindRaw) -> StateUpdateKindRaw {
        match self {
            ProtoVersion::V42 => {
                mz_catalog::durable::upgrade::objects_v42::StateUpdateKind::try_from(raw)
                    .unwrap()
                    .into()
            }
            ProtoVersion::V43 => {
                mz_catalog::durable::upgrade::objects_v43::StateUpdateKind::try_from(raw)
                    .unwrap()
                    .into()
            }
            ProtoVersion::V44 => {
                mz_catalog::durable::upgrade::objects_v44::StateUpdateKind::try_from(raw)
                    .unwrap()
                    .into()
            }
            ProtoVersion::V45 => {
                mz_catalog::durable::upgrade::objects_v45::StateUpdateKind::try_from(raw)
                    .unwrap()
                    .into()
            }
        }
    }
}
