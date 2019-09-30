// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use pretty::{BoxDoc, Doc};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Result};

/// An identifier.
///
/// A fair number of optimizations create, modify, or delete local bindings
/// and thus require a source of fresh identifiers. That, in turn, implies
/// that textual identifiers are of limited utility. While identifiers started
/// out as strings, they were originally created by converting a UUID to its
/// textual representation. In light of this use, it makes sense to outright
/// use an integer, wrapped as a newtype. As added benefit, this representation
/// is guaranteed to have a fixed, small size. It also is readily copyable.
/// Since identifiers abstract over a numeric value and are generated in
/// sequence, they also implement many of the Rust core traits, including
/// those for copying, ordering, and hashing.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Identifier(usize);

impl Identifier {
    /// Create a new identifier with the given index. It is tempting to define
    /// `From<usize> to minimize the notational overhead of creating new
    /// identifiers. But given the rather large semantic difference, this
    /// method being a mouthful is a feature, not a bug.
    pub fn from_index(index: usize) -> Self {
        Identifier(index)
    }
}

impl Display for Identifier {
    /// Nicely format this identifier.
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "id-{}", self.0)
    }
}

impl<'a> From<&Identifier> for Doc<'a, BoxDoc<'a, ()>, ()> {
    /// Convert an identifier into a document during pretty-printing.
    fn from(id: &Identifier) -> Doc<'a, BoxDoc<'a, ()>, ()> {
        Doc::as_string(id)
    }
}

/// An identifier forge.
///
/// This structure encapsulates the state and functionality for creating fresh
/// identifiers, i.e., each identifier is guaranteed to be different from all
/// other identifiers created by the same forge. Since identifiers are forged
/// one after the other, they also form an obvious total order. Each
/// identifier's one-based index in that total order coincides with the
/// identifier's value.
#[derive(Debug, Default)]
pub struct IdentifierForge {
    counter: usize,
}

impl IdentifierForge {
    /// Create a fresh identifier.
    pub fn fresh(&mut self) -> Identifier {
        self.counter += 1;
        Identifier(self.counter)
    }
}
