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

//! ID generation utilities.

/// Manages the allocation of unique IDs.
#[derive(Debug, Default)]
pub struct IdGen {
    id: u64,
}

impl IdGen {
    /// Allocates a new identifier and advances the generator.
    pub fn allocate_id(&mut self) -> u64 {
        let id = self.id;
        self.id += 1;
        id
    }
}
