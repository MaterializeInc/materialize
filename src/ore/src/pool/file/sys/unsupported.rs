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

//! File mode is Linux-only: every operation fails as unsupported, so
//! [`FileStore::open`](crate::pool::file::FileStore::open) does.

use std::fs::File;
use std::io;
use std::path::Path;

fn unsupported() -> io::Error {
    io::Error::new(io::ErrorKind::Unsupported, "file-backed extents need Linux")
}

pub(in crate::pool::file) fn is_memory_backed(_path: &Path) -> io::Result<bool> {
    Err(unsupported())
}

pub(in crate::pool::file) fn available_bytes(_path: &Path) -> io::Result<u64> {
    Err(unsupported())
}

pub(in crate::pool::file) fn open_anonymous(
    _dir: &Path,
    _class: usize,
    _direct: bool,
) -> io::Result<File> {
    Err(unsupported())
}

pub(in crate::pool::file) fn fallocate(_file: &File, _offset: u64, _len: usize) -> io::Result<()> {
    Err(unsupported())
}

pub(in crate::pool::file) fn punch_hole(_file: &File, _offset: u64, _len: usize) -> io::Result<()> {
    Err(unsupported())
}

pub(in crate::pool::file) fn pwrite(_file: &File, _buf: &[u8], _offset: u64) -> io::Result<usize> {
    Err(unsupported())
}

pub(in crate::pool::file) fn pread(
    _file: &File,
    _buf: &mut [u8],
    _offset: u64,
) -> io::Result<usize> {
    Err(unsupported())
}

pub(in crate::pool::file) fn writeback_and_drop(
    _file: &File,
    _offset: u64,
    _len: usize,
) -> io::Result<()> {
    Err(unsupported())
}

pub(in crate::pool::file) fn drop_cache(_file: &File, _offset: u64, _len: usize) -> io::Result<()> {
    Err(unsupported())
}
