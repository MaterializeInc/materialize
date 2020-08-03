// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use lazy_static::lazy_static;

use jemalloc_ctl::raw;
use std::os::unix::ffi::OsStrExt;
use std::sync::Arc;
use std::sync::Mutex;
use std::{
    ffi::{CString},
    time::Instant,
};
use tempfile::NamedTempFile;

#[derive(Copy, Clone, Debug)]
pub enum ProfStartTime {
    Instant(Instant),
    TimeImmemorial,
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

#[cfg(target_os = "macos")]
impl JemallocProfCtl {
    fn get() -> Option<Self> {
        None
    }
    pub fn get_md(&self) -> JemallocProfMetadata {
        unreachable!()
    }

    pub fn activate(&mut self) -> Result<(), jemalloc_ctl::Error> {
        unreachable!()
    }

    pub fn deactivate(&mut self) -> Result<(), jemalloc_ctl::Error> {
        unreachable!()
    }

    pub fn dump(&mut self) -> anyhow::Result<std::fs::File> {
        unreachable!()
    }
}

#[cfg(not(target_os = "macos"))]
impl JemallocProfCtl {
    // Creates and returns the global singleton.
    // This must only be called once - from the static initializer of
    // `PROF_METADATA`.
    fn get() -> Option<Self> {
        let prof_enabled: bool = unsafe { raw::read(b"opt.prof\0") }.unwrap();
        if prof_enabled {
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
        unsafe { raw::write(b"prof.active\0", true) }?;
        if self.md.start_time.is_none() {
            self.md.start_time = Some(ProfStartTime::Instant(Instant::now()));
        }
        Ok(())
    }

    pub fn deactivate(&mut self) -> Result<(), jemalloc_ctl::Error> {
        unsafe { raw::write(b"prof.active\0", false) }?;
        self.md.start_time = None;
        Ok(())
    }

    pub fn dump(&mut self) -> anyhow::Result<std::fs::File> {
        let f = NamedTempFile::new()?;
        let path = CString::new(f.path().as_os_str().as_bytes().to_vec()).unwrap();

        unsafe { raw::write(b"prof.dump\0", path.as_ptr()) }?;
        Ok(f.into_file())
    }
}

lazy_static! {
    pub static ref PROF_METADATA: Option<Arc<Mutex<JemallocProfCtl>>> = {
        if let Some(md) = JemallocProfCtl::get() {
            Some(Arc::new(Mutex::new(md)))
        } else {
            None
        }
    };
}
