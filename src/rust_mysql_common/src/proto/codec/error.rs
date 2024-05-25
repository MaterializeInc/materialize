// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::{fmt, io};

#[derive(Debug)]
pub enum PacketCodecError {
    Io(io::Error),
    PacketTooLarge,
    PacketsOutOfSync,
    BadCompressedPacketHeader,
}

impl From<io::Error> for PacketCodecError {
    fn from(io_err: io::Error) -> Self {
        Self::Io(io_err)
    }
}

impl fmt::Display for PacketCodecError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PacketCodecError::Io(io_err) => f.write_fmt(format_args!("IO error: `{}'", io_err)),
            PacketCodecError::PacketTooLarge => {
                f.write_str("Packet is larger than max_allowed_packet")
            }
            PacketCodecError::PacketsOutOfSync => f.write_str("Packets out of sync"),
            PacketCodecError::BadCompressedPacketHeader => {
                f.write_str("Bad compressed packet header")
            }
        }
    }
}

impl std::error::Error for PacketCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PacketCodecError::Io(io_err) => Some(io_err),
            _other => None,
        }
    }
}
