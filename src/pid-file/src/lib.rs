// Copyright 2020 Andrej Shadura.
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
//
// This file is derived from the bsd-pidfile-rs project, available at
// https://github.com/andrewshadura/bsd-pidfile-rs. It was incorporated
// directly into Materialize on August 12, 2020.
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.

//! PID file management for daemons.
//!
//! The `pid-file` crate wraps the `pidfile` family of functions provided by BSD
//! systems that provide mutual exclusion for daemons via PID files.
//!
//! Much of the code is inherited from [pidfile_rs], but the build system
//! bundles the necessary subset of libbsd rather than relying on libbsd to be
//! installed.
//!
//! [pidfile_rs]: https://docs.rs/pidfile_rs

#![warn(missing_docs)]

use libc::FD_CLOEXEC;
use std::ffi::{CString, NulError};
use std::fmt;
use std::fs::{OpenOptions, Permissions};
use std::io;
use std::io::{BufRead, BufReader};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::ptr;

use mz_ore::option::OptionExt;

#[allow(non_camel_case_types)]
#[repr(C)]
struct pidfh {
    pf_fd: libc::c_int,
    private: [u8; 0],
}

extern "C" {
    fn pidfile_open(
        path: *const libc::c_char,
        mode: libc::mode_t,
        pid: *const libc::pid_t,
    ) -> *mut pidfh;
    fn pidfile_write(pfh: *mut pidfh) -> libc::c_int;
    fn pidfile_remove(pfh: *mut pidfh) -> libc::c_int;
}

/// An open PID file.
///
/// A process that manages to construct this type holds an exclusive lock on the
/// PID file.
///
/// Dropping the type will attempt to call [`remove`](PidFile::remove), but any
/// errors will be suppressed. Call `remove` manually if you need to handle PID
/// file removal errors.
pub struct PidFile(*mut pidfh);

impl PidFile {
    /// Attempts to open and lock the specified PID file.
    ///
    /// If the file is already locked by another process, it returns
    /// `Error::AlreadyRunning`.
    pub fn open<P>(path: P) -> Result<PidFile, Error>
    where
        P: AsRef<Path>,
    {
        PidFile::open_with(path, Permissions::from_mode(0o600))
    }

    /// Like [`open`](PidFile::open), but opens the file with the specified
    /// permissions rather than 0600.
    #[allow(clippy::unnecessary_mut_passed)] // this mut is being passed as a mut pointer
    pub fn open_with<P>(path: P, permissions: Permissions) -> Result<PidFile, Error>
    where
        P: AsRef<Path>,
    {
        let path_cstring = CString::new(path.as_ref().as_os_str().as_bytes())?;
        let mut old_pid: libc::pid_t = -1;

        #[allow(clippy::useless_conversion)] // the types differ on macos/linux
        let mode: libc::mode_t = permissions
            .mode()
            .try_into()
            .expect("file permissions not valid libc::mode_t");
        let f = unsafe { pidfile_open(path_cstring.as_ptr(), mode, &mut old_pid) };
        if !f.is_null() {
            let r = unsafe { libc::fcntl((*f).pf_fd, libc::F_SETFD, FD_CLOEXEC) };
            if r != 0 {
                return Err(Error::Io(io::Error::last_os_error()));
            }

            let r = unsafe { pidfile_write(f) };
            if r == 0 {
                Ok(PidFile(f))
            } else {
                Err(Error::Io(io::Error::last_os_error()))
            }
        } else {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::AlreadyExists {
                Err(Error::AlreadyRunning {
                    pid: (old_pid != -1).then_some(old_pid),
                })
            } else {
                Err(Error::Io(err))
            }
        }
    }

    /// Closes the PID file and removes it from the filesystem.
    pub fn remove(mut self) -> Result<(), Error> {
        let r = unsafe { pidfile_remove(self.0) };
        // Set the pointer to null to prevent drop from calling remove again.
        self.0 = ptr::null_mut();
        if r == 0 {
            Ok(())
        } else {
            Err(Error::Io(io::Error::last_os_error()))
        }
    }

    /// Reads contents of PID file
    pub fn read<P>(path: P) -> Result<i32, Error>
    where
        P: AsRef<Path>,
    {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        let pid: i32 = reader
            .lines()
            .next()
            .expect("empty pid file")?
            .parse()
            .expect("malformed pid");

        Ok(pid)
    }
}

impl Drop for PidFile {
    fn drop(&mut self) {
        // If the pointer is null, the PID file has already been removed.
        if self.0 != ptr::null_mut() {
            unsafe { pidfile_remove(self.0) };
        }
    }
}

/// A PID file-related error.
#[derive(Debug)]
pub enum Error {
    /// An I/O error occurred.
    Io(io::Error),
    /// The provided path had embedded null bytes.
    Nul(NulError),
    /// Another process already has the lock on the requested PID file.
    AlreadyRunning {
        /// The PID of the existing process, if it is known.
        pid: Option<i32>,
    },
}

impl From<NulError> for Error {
    fn from(e: NulError) -> Error {
        Error::Nul(e)
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "unable to open PID file: {}", e),
            Error::Nul(e) => write!(f, "PID file path contained null bytes: {}", e),
            Error::AlreadyRunning { pid } => write!(
                f,
                "process already running (PID: {})",
                pid.display_or("<unknown>")
            ),
        }
    }
}

impl std::error::Error for Error {}
