// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Functions for working with permutations

use std::collections::HashMap;

/// Given a permutation, construct its inverse.
pub fn invert<I>(permutation: I) -> impl Iterator<Item = (usize, usize)>
where
    I: IntoIterator<Item = usize>,
{
    permutation.into_iter().enumerate().map(|(idx, c)| (c, idx))
}

/// Construct the permutation that sorts `data`.
pub fn argsort<T>(data: &[T]) -> Vec<usize>
where
    T: Ord,
{
    let mut indices = (0..data.len()).collect::<Vec<_>>();
    indices.sort_by_key(|&i| &data[i]);
    indices
}

/// Construct the permutation that takes `data.sorted()` to `data`.
pub fn inverse_argsort<T>(data: &[T]) -> Vec<usize>
where
    T: Ord,
{
    let map = invert(argsort(data)).collect::<HashMap<_, _>>();
    (0..data.len()).map(|i| map[&i]).collect()
}
