// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

//! Vector extensions.

/// Extension methods for [`std::vec::Vec`].
pub trait VecExt<T> {
    /// Consumes the vector and returns its first element.
    ///
    /// This method panics if the vector does not have at least one element.
    fn into_element(self) -> T;
}

impl<T> VecExt<T> for Vec<T> {
    fn into_element(self) -> T {
        self.into_iter().next().unwrap()
    }
}