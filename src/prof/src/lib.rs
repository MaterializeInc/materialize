// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{collections::BTreeMap, ffi::c_void, sync::atomic::AtomicBool, time::Instant};

pub mod http;
#[cfg(all(not(target_os = "macos"), feature = "jemalloc"))]
pub mod jemalloc;
pub mod time;

#[derive(Copy, Clone, Debug)]
// These constructors are dead on macOS
#[allow(dead_code)]
pub enum ProfStartTime {
    Instant(Instant),
    TimeImmemorial,
}

#[derive(Clone, Debug)]
pub struct WeightedStack {
    pub addrs: Vec<usize>,
    pub weight: f64,
}

#[derive(Default)]
pub struct StackProfile {
    annotations: Vec<String>,
    // The second element is the index in `annotations`, if one exists.
    stacks: Vec<(WeightedStack, Option<usize>)>,
}

impl StackProfile {
    /// Writes out the `.mzfg` format, which is fully described in flamegraph.js.
    pub fn to_mzfg(&self, symbolicate: bool, header_extra: &[(&str, &str)]) -> String {
        // All the unwraps in this function are justified by the fact that
        // String's fmt::Write impl is infallible.
        use std::fmt::Write;
        let mut builder = "mz_fg_version: 1\n".to_owned();
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
    pub fn push(&mut self, stack: WeightedStack, annotation: Option<&str>) {
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
            backtrace::resolve(addr as *mut c_void, |sym| {
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
