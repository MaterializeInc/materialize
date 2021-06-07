// Copyright Materialize, Inc. All rights reserved.
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

use std::error::Error;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};

/// The maximum allowable size of a frame in a framed stream.
pub const MAX_FRAME_SIZE: usize = 64 << 20;

/// An error indicating that a frame in a framed stream exceeded
/// [`MAX_FRAME_SIZE`].
pub struct FrameTooBig;

impl Debug for FrameTooBig {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("FrameTooBig").finish()
    }
}

impl Display for FrameTooBig {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.write_str("frame size too big")
    }
}

impl Error for FrameTooBig {}
