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

//! The protocol core of the Materialize SDK.
//!
//! The core holds everything about consuming `SUBSCRIBE` that has to behave
//! the same in every language, and performs no I/O. A language package runs
//! the statements the core builds over that language's own driver and passes
//! the rows it fetches back in.

#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::unreachable,
        clippy::todo,
        clippy::unimplemented,
        clippy::arithmetic_side_effects,
    )
)]

pub mod decode;
pub mod error;
pub mod frontier;
mod wire;

pub use decode::{Change, Datum, Decoder, Envelope, Message, Row, Update};
pub use error::ProtocolError;
pub use frontier::Frontier;
