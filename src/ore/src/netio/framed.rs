// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Apache license, Version 2.0

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
