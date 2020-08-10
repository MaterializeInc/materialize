// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//!
//! Various profiling utilities:
//!
//! (1) Turn jemalloc profiling on and off, and dump heap profiles (`PROF_CTL`)
//! (2) Parse jemalloc heap files and make them into a hierarchical format (`parse_jeheap` and `collate_stacks`)

use std::os::unix::ffi::OsStrExt;
use std::sync::Arc;
use std::sync::Mutex;
use std::{ffi::CString, io::BufRead, time::Instant};

use jemalloc_ctl::raw;
use lazy_static::lazy_static;
use tempfile::NamedTempFile;

use anyhow::bail;
use prof_common::{ProfStartTime, StackProfile, WeightedStack};

lazy_static! {
    pub static ref PROF_CTL: Option<Arc<Mutex<JemallocProfCtl>>> = {
        if let Some(ctl) = JemallocProfCtl::get() {
            Some(Arc::new(Mutex::new(ctl)))
        } else {
            None
        }
    };
}

#[derive(Copy, Clone, Debug)]
pub struct JemallocProfMetadata {
    pub start_time: Option<ProfStartTime>,
}

#[derive(Debug)]
// Per-process singleton object allowing control of jemalloc profiling facilities.
pub struct JemallocProfCtl {
    md: JemallocProfMetadata,
}

/// Parse a jemalloc profile file, producing a vector of stack traces along with their weights.
pub fn parse_jeheap<R: BufRead>(r: R) -> anyhow::Result<StackProfile> {
    let mut cur_stack = None;
    let mut profile = <StackProfile as Default>::default();
    for line in r.lines().into_iter() {
        let line = line?;
        let line = line.trim();
        let words = line.split_ascii_whitespace().collect::<Vec<_>>();
        if words.len() > 0 && words[0] == "@" {
            if cur_stack.is_some() {
                bail!("Stack without corresponding weight!")
            }
            let mut addrs = words[1..]
                .iter()
                .map(|w| {
                    let raw = w.trim_start_matches("0x");
                    usize::from_str_radix(raw, 16)
                })
                .collect::<Result<Vec<_>, _>>()?;
            addrs.reverse();
            cur_stack = Some(addrs);
        }
        if words.len() > 2 && words[0] == "t*:" {
            if let Some(addrs) = cur_stack.take() {
                let weight = str::parse::<usize>(words[2])?;
                profile.push(WeightedStack { addrs, weight }, None);
            }
        }
    }
    if cur_stack.is_some() {
        bail!("Stack without corresponding weight!")
    }
    Ok(profile)
}

impl JemallocProfCtl {
    // Creates and returns the global singleton.
    fn get() -> Option<Self> {
        // SAFETY: "opt.prof" is documented as being readable and returning a bool:
        // http://jemalloc.net/jemalloc.3.html#opt.prof
        let prof_enabled: bool = unsafe { raw::read(b"opt.prof\0") }.unwrap();
        if prof_enabled {
            // SAFETY: "opt.prof_active" is documented as being readable and returning a bool:
            // http://jemalloc.net/jemalloc.3.html#opt.prof_active
            let prof_active: bool = unsafe { raw::read(b"opt.prof_active\0") }.unwrap();
            let start_time = if prof_active {
                Some(ProfStartTime::TimeImmemorial)
            } else {
                None
            };
            let md = JemallocProfMetadata { start_time };
            Some(Self { md })
        } else {
            None
        }
    }

    pub fn get_md(&self) -> JemallocProfMetadata {
        self.md
    }

    pub fn activate(&mut self) -> Result<(), jemalloc_ctl::Error> {
        // SAFETY: "prof.active" is documented as being writable and taking a bool:
        // http://jemalloc.net/jemalloc.3.html#prof.active
        unsafe { raw::write(b"prof.active\0", true) }?;
        if self.md.start_time.is_none() {
            self.md.start_time = Some(ProfStartTime::Instant(Instant::now()));
        }
        Ok(())
    }

    pub fn deactivate(&mut self) -> Result<(), jemalloc_ctl::Error> {
        // SAFETY: "prof.active" is documented as being writable and taking a bool:
        // http://jemalloc.net/jemalloc.3.html#prof.active
        unsafe { raw::write(b"prof.active\0", false) }?;
        self.md.start_time = None;
        Ok(())
    }

    pub fn dump(&mut self) -> anyhow::Result<std::fs::File> {
        let f = NamedTempFile::new()?;
        let path = CString::new(f.path().as_os_str().as_bytes().to_vec()).unwrap();

        // SAFETY: "prof.dump" is documented as being writable and taking a C string as input:
        // http://jemalloc.net/jemalloc.3.html#prof.dump
        unsafe { raw::write(b"prof.dump\0", path.as_ptr()) }?;
        Ok(f.into_file())
    }
}
