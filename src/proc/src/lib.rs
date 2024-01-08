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

//! Utility crate to extract information about the running process.
//!
//! Currently only works on Linux.
use std::fmt;
use std::path::PathBuf;

#[cfg(target_os = "linux")]
pub mod linux;

/// Information about a shared object loaded into the current process.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SharedObject {
    /// The address at which the object is loaded.
    pub base_address: usize,
    /// The path of that file the object was loaded from.
    pub path_name: PathBuf,
    /// The build ID of the object, if found.
    pub build_id: Option<BuildId>,
    /// Loaded segments of the object.
    pub loaded_segments: Vec<LoadedSegment>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct BuildId(Vec<u8>);

impl fmt::Display for BuildId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// A segment of a shared object that's loaded into memory.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct LoadedSegment {
    /// Offset of the segment in the source file.
    pub file_offset: u64,
    /// Offset to the `SharedObject`'s `base_address`.
    pub memory_offset: usize,
    /// Size of the segment in memory.
    pub memory_size: usize,
}
