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
use std::error::Error;

use mz_orchestrator_process::port_metadata_file::PortMetadataFile;

#[test]
fn test_port_metadata_file_basic() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("portfile");

    let port_metadata: HashMap<String, i32> =
        vec![("joe".to_string(), 42), ("shmoe".to_string(), 666)]
            .into_iter()
            .collect();
    let _port_metadata_file = PortMetadataFile::open(&path, &port_metadata)?;
    assert!(path.exists());

    let port_metadata_file_contents = PortMetadataFile::read(&path)?;

    assert_eq!(port_metadata, port_metadata_file_contents);

    Ok(())
}
