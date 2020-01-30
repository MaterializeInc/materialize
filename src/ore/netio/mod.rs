// Copyright 2019-2020 Materialize Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Network I/O utilities.

mod framed;
mod read_exact;
mod stream;

pub use self::framed::{FrameTooBig, MAX_FRAME_SIZE};
pub use self::read_exact::{read_exact_or_eof, ReadExactOrEof};
pub use self::stream::{SniffedStream, SniffingStream};
