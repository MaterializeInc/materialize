// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::io;

/// A communication error.
pub struct Error(ErrorKind);

enum ErrorKind {
    Bincode(bincode::Error),
    OneshotCanceled(futures::channel::oneshot::Canceled),
    MpscSend(futures::channel::mpsc::SendError),
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.0 {
            ErrorKind::Bincode(err) => Some(err),
            ErrorKind::OneshotCanceled(err) => Some(err),
            ErrorKind::MpscSend(err) => Some(err),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("comm error: ")?;
        match &self.0 {
            ErrorKind::Bincode(err) => write!(f, "bincode/io error: {}", err),
            ErrorKind::OneshotCanceled(_) => f.write_str("oneshot canceled"),
            ErrorKind::MpscSend(err) => write!(f, "mpsc send failed: {}", err),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Error {
        Error(ErrorKind::Bincode(err))
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error(ErrorKind::Bincode(bincode::Error::from(err)))
    }
}

impl From<futures::channel::oneshot::Canceled> for Error {
    fn from(err: futures::channel::oneshot::Canceled) -> Error {
        Error(ErrorKind::OneshotCanceled(err))
    }
}

impl From<futures::channel::mpsc::SendError> for Error {
    fn from(err: futures::channel::mpsc::SendError) -> Error {
        Error(ErrorKind::MpscSend(err))
    }
}
