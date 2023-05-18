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

//! Network I/O utilities.

mod async_ready;
mod framed;
mod read_exact;
mod socket;

pub use crate::netio::async_ready::AsyncReady;
pub use crate::netio::framed::{FrameTooBig, MAX_FRAME_SIZE};
pub use crate::netio::read_exact::{read_exact_or_eof, ReadExactOrEof};
pub use crate::netio::socket::{Listener, SocketAddr, SocketAddrType, Stream, UnixSocketAddr};
