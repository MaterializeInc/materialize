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

use std::collections::BTreeMap;
use std::ffi::c_void;
use std::io::Write;
use std::sync::atomic::AtomicBool;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use flate2::write::GzEncoder;
use flate2::Compression;
use mz_build_id::BuildId;
use mz_ore::cast::{CastFrom, TryCastFrom};
use prost::Message;

mod pprof_types;
mod time;

pub mod http;
#[cfg(feature = "jemalloc")]
pub mod jemalloc;

#[derive(Copy, Clone, Debug)]
pub enum ProfStartTime {
    Instant(Instant),
    TimeImmemorial,
}

#[derive(Clone, Debug)]
pub struct WeightedStack {
    pub addrs: Vec<usize>,
    pub weight: f64,
}

pub struct Mapping {
    pub memory_start: usize,
    pub memory_end: usize,
    pub file_offset: u64,
    pub pathname: Option<String>,
    pub build_id: Option<BuildId>,
}

#[derive(Default)]
pub struct StackProfile {
    annotations: Vec<String>,
    // The second element is the index in `annotations`, if one exists.
    stacks: Vec<(WeightedStack, Option<usize>)>,
    mappings: Vec<Mapping>,
}

impl StackProfile {
    /// Writes out the `.mzfg` format, which is fully described in flamegraph.js.
    pub fn to_mzfg(&self, symbolicate: bool, header_extra: &[(&str, &str)]) -> String {
        // All the unwraps in this function are justified by the fact that
        // String's fmt::Write impl is infallible.
        use std::fmt::Write;
        let mut builder = r#"!!! COMMENT !!!: Open with bin/fgviz /path/to/mzfg
mz_fg_version: 1
"#
        .to_owned();
        for (k, v) in header_extra {
            assert!(!(k.contains(':') || k.contains('\n') || v.contains('\n')));
            writeln!(&mut builder, "{k}: {v}").unwrap();
        }
        writeln!(&mut builder, "").unwrap();

        for (WeightedStack { addrs, weight }, anno) in &self.stacks {
            let anno = anno.map(|i| &self.annotations[i]);
            for &addr in addrs {
                write!(&mut builder, "{addr:#x};").unwrap();
            }
            write!(&mut builder, " {weight}").unwrap();
            if let Some(anno) = anno {
                write!(&mut builder, " {anno}").unwrap()
            }
            writeln!(&mut builder, "").unwrap();
        }

        if symbolicate {
            let symbols = crate::symbolicate(self);
            writeln!(&mut builder, "").unwrap();

            for (addr, names) in symbols {
                if !names.is_empty() {
                    write!(&mut builder, "{addr:#x} ").unwrap();
                    for mut name in names {
                        // The client splits on semicolons, so
                        // we have to escape them.
                        name = name.replace('\\', "\\\\");
                        name = name.replace(';', "\\;");
                        write!(&mut builder, "{name};").unwrap();
                    }
                    writeln!(&mut builder, "").unwrap();
                }
            }
        }

        builder
    }

    /// Converts the profile into the pprof format.
    ///
    /// pprof encodes profiles as gzipped protobuf messages of the Profile message type
    /// (see `pprof/profile.proto`).
    pub fn to_pprof(
        &self,
        sample_type: (&str, &str),
        period_type: (&str, &str),
        anno_key: Option<String>,
    ) -> Vec<u8> {
        use crate::pprof_types as proto;

        let mut profile = proto::Profile::default();

        const SAMPLE_TYPE_IDX: i64 = 1;
        const SAMPLE_UNIT_IDX: i64 = 2;
        const PERIOD_TYPE_IDX: i64 = 3;
        const PERIOD_UNIT_IDX: i64 = 4;
        const ANNO_KEY_IDX: i64 = 5;

        let anno_key = anno_key.unwrap_or_else(|| "annotation".into());
        profile.string_table = vec![
            "".into(),
            sample_type.0.into(),
            sample_type.1.into(),
            period_type.0.into(),
            period_type.1.into(),
            anno_key,
        ];

        profile.sample_type = vec![proto::ValueType {
            r#type: SAMPLE_TYPE_IDX,
            unit: SAMPLE_UNIT_IDX,
        }];
        profile.period_type = Some(proto::ValueType {
            r#type: PERIOD_TYPE_IDX,
            unit: PERIOD_UNIT_IDX,
        });

        profile.time_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("now is later than UNIX epoch")
            .as_nanos()
            .try_into()
            .expect("the year 2554 is far away");

        let mut filename_indices = BTreeMap::new();
        let mut build_id_indices = BTreeMap::new();
        for (mapping, mapping_id) in self.mappings.iter().zip(1..) {
            let pathname = mapping.pathname.as_deref().unwrap_or("");
            let filename_idx = *filename_indices.entry(pathname).or_insert_with(|| {
                let index = profile.string_table.len();
                profile.string_table.push(pathname.to_string());
                i64::try_from(index).expect("must fit")
            });

            let build_id_idx = match &mapping.build_id {
                Some(build_id) => *build_id_indices
                    .entry(&mapping.build_id)
                    .or_insert_with(|| {
                        let index = profile.string_table.len();
                        profile.string_table.push(build_id.to_string());
                        i64::try_from(index).expect("must fit")
                    }),
                None => 0,
            };

            profile.mapping.push(proto::Mapping {
                id: mapping_id,
                memory_start: u64::cast_from(mapping.memory_start),
                memory_limit: u64::cast_from(mapping.memory_end),
                file_offset: mapping.file_offset,
                filename: filename_idx,
                build_id: build_id_idx,
                ..Default::default()
            });
        }

        let mut location_ids = BTreeMap::new();
        let mut anno_indices = BTreeMap::new();
        for (stack, anno) in self.iter() {
            let mut sample = proto::Sample::default();

            let value = stack.weight.trunc();
            let value = i64::try_cast_from(value).expect("no exabyte heap sizes");
            sample.value.push(value);

            for addr in stack.addrs.iter().rev() {
                let addr = u64::cast_from(*addr);

                let loc_id = *location_ids.entry(addr).or_insert_with(|| {
                    // pprof_types.proto says the location id may be the address, but Polar Signals
                    // insists that location ids are sequential, starting with 1.
                    let id = u64::cast_from(profile.location.len()) + 1;
                    let mapping_id = profile
                        .mapping
                        .iter()
                        .find(|m| m.memory_start <= addr && m.memory_limit > addr)
                        .map_or(0, |m| m.id);
                    profile.location.push(proto::Location {
                        id,
                        mapping_id,
                        address: addr,
                        ..Default::default()
                    });
                    id
                });

                sample.location_id.push(loc_id);

                if let Some(anno) = anno {
                    let index = anno_indices.entry(anno).or_insert_with(|| {
                        let index = profile.string_table.len();
                        profile.string_table.push(anno.into());
                        i64::try_from(index).expect("must fit")
                    });
                    sample.label.push(proto::Label {
                        key: ANNO_KEY_IDX,
                        str: *index,
                        ..Default::default()
                    })
                }
            }

            profile.sample.push(sample);
        }

        let encoded = profile.encode_to_vec();

        let mut gz = GzEncoder::new(Vec::new(), Compression::default());
        gz.write_all(&encoded).unwrap();
        gz.finish().unwrap()
    }
}

pub struct StackProfileIter<'a> {
    inner: &'a StackProfile,
    idx: usize,
}

impl<'a> Iterator for StackProfileIter<'a> {
    type Item = (&'a WeightedStack, Option<&'a str>);

    fn next(&mut self) -> Option<Self::Item> {
        let (stack, anno) = self.inner.stacks.get(self.idx)?;
        self.idx += 1;
        let anno = anno.map(|idx| self.inner.annotations.get(idx).unwrap().as_str());
        Some((stack, anno))
    }
}

impl StackProfile {
    pub fn push_stack(&mut self, stack: WeightedStack, annotation: Option<&str>) {
        let anno_idx = if let Some(annotation) = annotation {
            Some(
                self.annotations
                    .iter()
                    .position(|anno| annotation == anno.as_str())
                    .unwrap_or_else(|| {
                        self.annotations.push(annotation.to_string());
                        self.annotations.len() - 1
                    }),
            )
        } else {
            None
        };
        self.stacks.push((stack, anno_idx))
    }

    pub fn push_mapping(&mut self, mapping: Mapping) {
        self.mappings.push(mapping);
    }

    pub fn iter(&self) -> StackProfileIter<'_> {
        StackProfileIter {
            inner: self,
            idx: 0,
        }
    }
}

static EVER_SYMBOLICATED: AtomicBool = AtomicBool::new(false);

/// Check whether symbolication has ever been run in this process.
/// This controls whether we display a warning about increasing RAM usage
/// due to the backtrace cache on the
/// profiler page. (Because the RAM hit is one-time, we don't need to warn if it's already happened).
pub fn ever_symbolicated() -> bool {
    EVER_SYMBOLICATED.load(std::sync::atomic::Ordering::SeqCst)
}

/// Given some stack traces, generate a map of addresses to their
/// corresponding symbols.
///
/// Each address could correspond to more than one symbol, because
/// of inlining. (E.g. if 0x1234 comes from "g", which is inlined in "f", the corresponding vec of symbols will be ["f", "g"].)
pub fn symbolicate(profile: &StackProfile) -> BTreeMap<usize, Vec<String>> {
    EVER_SYMBOLICATED.store(true, std::sync::atomic::Ordering::SeqCst);
    let mut all_addrs = vec![];
    for (stack, _annotation) in profile.stacks.iter() {
        all_addrs.extend(stack.addrs.iter().cloned());
    }
    // Sort so addresses from the same images are together,
    // to avoid thrashing `backtrace::resolve`'s cache of
    // parsed images.
    all_addrs.sort_unstable();
    all_addrs.dedup();
    all_addrs
        .into_iter()
        .map(|addr| {
            let mut syms = vec![];
            // No other known way to convert usize to pointer.
            #[allow(clippy::as_conversions)]
            let addr_ptr = addr as *mut c_void;
            backtrace::resolve(addr_ptr, |sym| {
                let name = sym
                    .name()
                    .map(|sn| sn.to_string())
                    .unwrap_or_else(|| "???".to_string());
                syms.push(name);
            });
            syms.reverse();
            (addr, syms)
        })
        .collect()
}
