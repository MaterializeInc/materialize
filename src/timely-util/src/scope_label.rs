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

//! Scopes with profiling labels set at schedule time.

use timely::dataflow::Scope;
use timely::progress::Timestamp;

/// Extension trait for timely [`Scope`] that allows one to convert a scope into one that sets its
/// name as a profiling label before scheduling its child operators.
pub trait ScopeExt: Sized {
    fn with_label(self) -> Self;
}

impl<'scope, T: Timestamp> ScopeExt for Scope<'scope, T> {
    fn with_label(self) -> Self {
        // TODO(mcsherry): figure out what this is meant to do, and help do it.
        // Restoring labelling via `scoped_raw` is the likely path forward.
        self
    }
}
