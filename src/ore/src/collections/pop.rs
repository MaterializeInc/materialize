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

//! Additional helper methods for collections that support `front` and `pop_front` and/or
//! `back` and `pop_back`.

use std::collections::VecDeque;

/// Helper methods for collections that support `front` and `pop_front`
pub trait PopFront {
    /// The item contained by the collection.
    type Item;
    /// Call a supplied function on each element at the
    /// front of the collection in turn, removing it if the
    /// function returns true, and stopping otherwise.
    fn pop_front_while<F: FnMut(&Self::Item) -> bool>(&mut self, f: F);
}

/// Helper methods for collections that support `back` and `pop_back`, or
/// similar methods (e.g. `last` and `pop` for `Vec`).
pub trait PopBack {
    // The item contained by the collection.
    type Item;
    /// Call a supplied function on each element at the
    /// back of the collection in turn, removing it if the
    /// function returns true, and stopping otherwise.
    fn pop_back_while<F: FnMut(&Self::Item) -> bool>(&mut self, f: F);
}

impl<T> PopFront for VecDeque<T> {
    type Item = T;
    fn pop_front_while<F: FnMut(&T) -> bool>(&mut self, mut f: F) {
        while let Some(item) = self.front() {
            if f(item) {
                self.pop_front();
            } else {
                break;
            }
        }
    }
}

impl<T> PopBack for VecDeque<T> {
    type Item = T;
    fn pop_back_while<F: FnMut(&T) -> bool>(&mut self, mut f: F) {
        while let Some(item) = self.back() {
            if f(item) {
                self.pop_back();
            } else {
                break;
            }
        }
    }
}

impl<T> PopBack for Vec<T> {
    type Item = T;
    fn pop_back_while<F: FnMut(&T) -> bool>(&mut self, mut f: F) {
        while let Some(item) = self.last() {
            if f(item) {
                self.pop();
            } else {
                break;
            }
        }
    }
}
