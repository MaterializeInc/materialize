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

//! Container builder wrapper that records bytes emitted for fuel-based yielding.

use std::cell::Cell;

use timely::container::{ContainerBuilder, PushInto};

/// A [`ContainerBuilder`] wrapper that carries a byte counter.
///
/// The wrapper forwards all container-builder operations verbatim; bytes are
/// accumulated externally through [`AsyncOutputHandle::give_fueled`][gf] so
/// the caller can use whatever size estimate is cheapest for their data
/// (e.g. `Row::byte_len`) without having to materialize the data into a
/// columnated region.
///
/// [gf]: crate::builder_async::AsyncOutputHandle::give_fueled
#[derive(Default)]
pub struct AccountedBuilder<CB> {
    pub bytes: Cell<usize>,
    pub builder: CB,
}

impl<CB: ContainerBuilder> ContainerBuilder for AccountedBuilder<CB> {
    type Container = CB::Container;

    fn extract(&mut self) -> Option<&mut Self::Container> {
        self.builder.extract()
    }

    fn finish(&mut self) -> Option<&mut Self::Container> {
        self.builder.finish()
    }
}

impl<T, CB: PushInto<T>> PushInto<T> for AccountedBuilder<CB> {
    #[inline]
    fn push_into(&mut self, item: T) {
        self.builder.push_into(item);
    }
}
