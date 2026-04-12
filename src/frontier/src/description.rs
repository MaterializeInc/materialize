// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License").

//! A description of a batch of updates: lower/upper/since frontiers.

use serde::{Deserialize, Serialize};
use crate::Antichain;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(bound(serialize = "T: Serialize", deserialize = "T: Deserialize<'de>"))]
pub struct Description<T> {
    lower: Antichain<T>,
    upper: Antichain<T>,
    since: Antichain<T>,
}

impl<T> Description<T> {
    pub fn new(lower: Antichain<T>, upper: Antichain<T>, since: Antichain<T>) -> Self {
        Description { lower, upper, since }
    }
    pub fn lower(&self) -> &Antichain<T> { &self.lower }
    pub fn upper(&self) -> &Antichain<T> { &self.upper }
    pub fn since(&self) -> &Antichain<T> { &self.since }
}
