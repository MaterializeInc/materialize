// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Logical time.

use std::convert::TryFrom;
use std::time::Instant;

pub type Timestamp = u64;

/// A monotonic logical clock.
#[derive(Clone)]
pub struct Clock(Instant);

impl Clock {
    /// Creates a new clock.
    pub fn new() -> Clock {
        Clock(Instant::now())
    }

    /// Returns the current logical time. Calling `now()` multiple times may
    /// return the same logical time.
    pub fn now(&self) -> Timestamp {
        let ms = self.0.elapsed().as_millis();
        // It is inconceivable that we'd be unable to store the number of
        // elapsed milliseconds in a u64, but we check and panic, just in case.
        u64::try_from(ms).unwrap()
    }
}

impl Default for Clock {
    fn default() -> Clock {
        Clock::new()
    }
}
