// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Apache license, Version 2.0

//! Network I/O utilities.

mod async_ready;
mod framed;
mod read_exact;
mod stream;

pub use self::async_ready::AsyncReady;
pub use self::framed::{FrameTooBig, MAX_FRAME_SIZE};
pub use self::read_exact::{read_exact_or_eof, ReadExactOrEof};
pub use self::stream::{SniffedStream, SniffingStream};
