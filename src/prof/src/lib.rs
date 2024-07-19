// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::ffi::c_void;
use std::io::Write;
use std::sync::atomic::AtomicBool;
use std::time::{SystemTime, UNIX_EPOCH};

use flate2::write::GzEncoder;
use flate2::Compression;
use mz_ore::cast::{CastFrom, TryCastFrom};
use pprof_util::{StackProfile, WeightedStack};
use prost::Message;

mod pprof_types;
pub mod time;

#[cfg(feature = "jemalloc")]
pub mod jemalloc;

pub trait StackProfileExt {
    /// Writes out the `.mzfg` format, which is fully described in flamegraph.js.
    fn to_mzfg(&self, symbolize: bool, header_extra: &[(&str, &str)]) -> String;
    /// Converts the profile into the pprof format.
    ///
    /// pprof encodes profiles as gzipped protobuf messages of the Profile message type
    /// (see `pprof/profile.proto`).
    fn to_pprof(
        &self,
        sample_type: (&str, &str),
        period_type: (&str, &str),
        anno_key: Option<String>,
    ) -> Vec<u8>;
}

impl StackProfileExt for StackProfile {
    fn to_mzfg(&self, symbolize: bool, header_extra: &[(&str, &str)]) -> String {
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

        if symbolize {
            let symbols = crate::symbolize(self);
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

    fn to_pprof(
        &self,
        sample_type: (&str, &str),
        period_type: (&str, &str),
        anno_key: Option<String>,
    ) -> Vec<u8> {
        use crate::pprof_types as proto;

        let mut profile = proto::Profile::default();
        let mut strings = StringTable::new();

        let anno_key = anno_key.unwrap_or_else(|| "annotation".into());

        profile.sample_type = vec![proto::ValueType {
            r#type: strings.insert(sample_type.0),
            unit: strings.insert(sample_type.1),
        }];
        profile.period_type = Some(proto::ValueType {
            r#type: strings.insert(period_type.0),
            unit: strings.insert(period_type.1),
        });

        profile.time_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("now is later than UNIX epoch")
            .as_nanos()
            .try_into()
            .expect("the year 2554 is far away");

        for (mapping, mapping_id) in self.mappings.iter().zip(1..) {
            let pathname = mapping.pathname.to_string_lossy();
            let filename_idx = strings.insert(&pathname);

            let build_id_idx = match &mapping.build_id {
                Some(build_id) => strings.insert(&build_id.to_string()),
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

            // This is a is a Polar Signals-specific extension: For correct offline symbolization
            // they need access to the memory offset of mappings, but the pprof format only has a
            // field for the file offset. So we instead encode additional information about
            // mappings in magic comments. There must be exactly one comment for each mapping.

            // Take a shortcut and assume the ELF type is always `ET_DYN`. This is true for shared
            // libraries and for position-independent executable, so it should always be true for
            // any mappings we have.
            // Getting the actual information is annoying. It's in the ELF header (the `e_type`
            // field), but there is no guarantee that the full ELF header gets mapped, so we might
            // not be able to find it in memory. We could try to load it from disk instead, but
            // then we'd have to worry about blocking disk I/O.
            let elf_type = 3;

            let comment = format!(
                "executableInfo={:x};{:x};{:x}",
                elf_type, mapping.file_offset, mapping.memory_offset
            );
            profile.comment.push(strings.insert(&comment));
        }

        let mut location_ids = BTreeMap::new();
        for (stack, anno) in self.iter() {
            let mut sample = proto::Sample::default();

            let value = stack.weight.trunc();
            let value = i64::try_cast_from(value).expect("no exabyte heap sizes");
            sample.value.push(value);

            for addr in stack.addrs.iter().rev() {
                // See the comment
                // [here](https://github.com/rust-lang/backtrace-rs/blob/036d4909e1fb9c08c2bb0f59ac81994e39489b2f/src/symbolize/mod.rs#L123-L147)
                // for why we need to subtract one. tl;dr addresses
                // in stack traces are actually the return address of
                // the called function, which is one past the call
                // itself.
                //
                // Of course, the `call` instruction can be more than one byte, so after subtracting
                // one, we might point somewhere in the middle of it, rather
                // than to the beginning of the instruction. That's fine; symbolization
                // tools don't seem to get confused by this.
                let addr = u64::cast_from(*addr) - 1;

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
                    sample.label.push(proto::Label {
                        key: strings.insert(&anno_key),
                        str: strings.insert(anno),
                        ..Default::default()
                    })
                }
            }

            profile.sample.push(sample);
        }

        profile.string_table = strings.finish();

        let encoded = profile.encode_to_vec();

        let mut gz = GzEncoder::new(Vec::new(), Compression::default());
        gz.write_all(&encoded).unwrap();
        gz.finish().unwrap()
    }
}

/// Helper struct to simplify building a `string_table` for the pprof format.
#[derive(Default)]
struct StringTable(BTreeMap<String, i64>);

impl StringTable {
    fn new() -> Self {
        // Element 0 must always be the emtpy string.
        let inner = [("".into(), 0)].into();
        Self(inner)
    }

    fn insert(&mut self, s: &str) -> i64 {
        if let Some(idx) = self.0.get(s) {
            *idx
        } else {
            let idx = i64::try_from(self.0.len()).expect("must fit");
            self.0.insert(s.into(), idx);
            idx
        }
    }

    fn finish(self) -> Vec<String> {
        let mut vec: Vec<_> = self.0.into_iter().collect();
        vec.sort_by_key(|(_, idx)| *idx);
        vec.into_iter().map(|(s, _)| s).collect()
    }
}

static EVER_SYMBOLIZED: AtomicBool = AtomicBool::new(false);

/// Check whether symbolization has ever been run in this process.
/// This controls whether we display a warning about increasing RAM usage
/// due to the backtrace cache on the
/// profiler page. (Because the RAM hit is one-time, we don't need to warn if it's already happened).
pub fn ever_symbolized() -> bool {
    EVER_SYMBOLIZED.load(std::sync::atomic::Ordering::SeqCst)
}

/// Given some stack traces, generate a map of addresses to their
/// corresponding symbols.
///
/// Each address could correspond to more than one symbol, because
/// of inlining. (E.g. if 0x1234 comes from "g", which is inlined in "f", the corresponding vec of symbols will be ["f", "g"].)
pub fn symbolize(profile: &StackProfile) -> BTreeMap<usize, Vec<String>> {
    EVER_SYMBOLIZED.store(true, std::sync::atomic::Ordering::SeqCst);
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
