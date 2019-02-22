// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

//! Utilities for network I/O.

mod framed;
mod read_exact;
mod stream;

pub use self::framed::{FrameTooBig, MAX_FRAME_SIZE};
pub use self::read_exact::{read_exact_or_eof, ReadExactOrEof};
pub use self::stream::{SniffedStream, SniffingStream};
