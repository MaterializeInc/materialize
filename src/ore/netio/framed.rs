// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};

/// The maximum allowable size of a frame in a framed stream.
pub const MAX_FRAME_SIZE: u32 = 8 << 10;

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
        f.write_str(self.description())
    }
}

impl Error for FrameTooBig {
    fn description(&self) -> &str {
        "frame size too big"
    }
}
