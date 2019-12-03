// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::fmt;

use serde::{Deserialize, Serialize};

/// An opaque identifier for a dataflow component. In other words, identifies
/// the target of a [`RelationExpr::Get`](crate::RelationExpr::Get).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Id {
    /// An identifier that refers to a local component of a dataflow.
    Local(LocalId),
    /// An identifier that refers to a global dataflow.
    Global(GlobalId),
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Id::Local(id) => id.fmt(f),
            Id::Global(id) => id.fmt(f),
        }
    }
}

/// A trait for turning [`Id`]s into human-readable strings.
pub trait IdHumanizer {
    /// Attempts to return the a human-readable string for `id`.
    fn humanize_id(&self, id: Id) -> Option<String>;
}

/// The identifier for a local component of a dataflow.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct LocalId(usize);

impl LocalId {
    /// Constructs a new local identifier. It is the caller's responsibility
    /// to provide a unique `v`.
    pub fn new(v: usize) -> LocalId {
        LocalId(v)
    }
}

impl fmt::Display for LocalId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "l{}", self.0)
    }
}

/// The identifier for a global dataflow.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum GlobalId {
    /// System namespace.
    System(usize),
    /// User namespace.
    User(usize),
}

impl GlobalId {
    /// Constructs a new global identifier in the system namespace. It is the
    /// caller's responsibility to provide a unique `v`.
    pub fn system(v: usize) -> GlobalId {
        GlobalId::System(v)
    }

    /// Constructs a new global identifier in the user namespace. It is the
    /// caller's responsiblity to provide a unique `v`.
    pub fn user(v: usize) -> GlobalId {
        GlobalId::User(v)
    }
}

impl fmt::Display for GlobalId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GlobalId::System(id) => write!(f, "s{}", id),
            GlobalId::User(id) => write!(f, "u{}", id),
        }
    }
}

/// Humanizer that provides no additional information.
#[derive(Debug)]
pub struct DummyHumanizer;

impl IdHumanizer for DummyHumanizer {
    fn humanize_id(&self, _: Id) -> Option<String> {
        None
    }
}

#[cfg(test)]
pub mod test_utils {
    use super::*;

    pub struct DummyHumanizer;

    impl IdHumanizer for DummyHumanizer {
        fn humanize_id(&self, _: Id) -> Option<String> {
            None
        }
    }

    impl From<&LocalId> for char {
        fn from(id: &LocalId) -> char {
            id.0 as u8 as char
        }
    }
}
