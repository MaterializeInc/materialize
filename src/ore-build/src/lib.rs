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

//! Internal utility libraries used in Build Scripts for Materialize.
//!
//! This exists separately from the `ore` crate because when cross compiling build scripts get
//! built for the host architecture, because that's where they're executed. `ore` has _many_
//! dependencies yet nearly none of them are used in build scripts, so we end up building a lot of
//! unnecessary crates.
//!
//! Moving the common utilities for build scripts into this crate, considerably improves build
//! times.

pub mod codegen;
