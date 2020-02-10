// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Network I/O utilities.

mod framed;
mod read_exact;
mod stream;

pub use self::framed::{FrameTooBig, MAX_FRAME_SIZE};
pub use self::read_exact::{read_exact_or_eof, ReadExactOrEof};
pub use self::stream::{SniffedStream, SniffingStream};
