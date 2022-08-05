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

use std::collections::HashMap;
use std::ffi::NulError;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::{fmt, io};

/// Handle to a file that contains metadata about a processes port mappings.
///
/// This is not meant to be used in production, it is to help orchestrate
/// processes on local deployments, by accompanying a `PidFile`.
#[derive(Debug)]
pub struct PortMetadataFile<P: AsRef<Path>> {
    path: P,
}

impl<P: AsRef<Path>> PortMetadataFile<P> {
    /// Attempts to open and write the specified port metadata file.
    pub fn open(
        path: P,
        port_metadata: &HashMap<String, u16>,
    ) -> Result<PortMetadataFile<P>, Error> {
        let port_metadata = serde_json::to_string(&port_metadata)
            .unwrap_or_else(|_| panic!("failed to serialize {:?}", port_metadata));
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&path)?;
        write!(file, "{port_metadata}")?;
        Ok(PortMetadataFile { path })
    }

    /// Obtains handle to existing `PortMetadataFile`.
    pub fn open_existing(path: P) -> PortMetadataFile<P> {
        assert!(
            path.as_ref().exists(),
            "missing port metadata file: {}",
            path.as_ref().as_os_str().to_str().unwrap()
        );
        PortMetadataFile { path }
    }

    /// Reads the contents of a `PortMetadataFile`
    pub fn read(path: P) -> Result<HashMap<String, u16>, Error> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        let port_metadata = reader.lines().next().expect("empty port metadata file")?;
        Ok(serde_json::from_str(port_metadata.as_str()).expect("malformed port metadata"))
    }
}

impl<P: AsRef<Path>> Drop for PortMetadataFile<P> {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        // Best effort attempt to fsync the delete.
        if let Some(parent_path) = self.path.as_ref().parent() {
            let _ = std::fs::File::open(parent_path).and_then(|h| h.sync_all());
        }
    }
}

/// A port metadata file-related error.
#[derive(Debug)]
pub enum Error {
    /// An I/O error occurred.
    Io(io::Error),
    /// The provided path had embedded null bytes.
    Nul(NulError),
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
            Error::Io(e) => write!(f, "unable to open port metadata file: {}", e),
            Error::Nul(e) => write!(f, "port metadata file path contained null bytes: {}", e),
        }
    }
}

impl std::error::Error for Error {}
