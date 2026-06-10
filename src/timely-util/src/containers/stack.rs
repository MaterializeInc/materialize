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

//! Container builder wrapper that carries a byte counter for fuel-based yielding.

use std::cell::Cell;

use timely::container::{ContainerBuilder, PushInto};

/// A [`ContainerBuilder`] wrapper carrying a byte counter that
/// [`AsyncOutputHandle::give_fueled`][gf] increments when emitting items. The
/// wrapper itself does no accounting; it exists so `give_fueled` can find a
/// counter associated with the output handle. The caller supplies the size of
/// each pushed item, which lets the source choose whatever estimate is
/// cheapest for its data without needing the container to introspect items.
///
/// [gf]: crate::builder_async::AsyncOutputHandle::give_fueled
#[derive(Default)]
pub struct FueledBuilder<CB> {
    pub bytes: Cell<usize>,
    pub builder: CB,
}

impl<CB: ContainerBuilder> ContainerBuilder for FueledBuilder<CB> {
    type Container = CB::Container;

    fn extract(&mut self) -> Option<&mut Self::Container> {
        self.builder.extract()
    }

    fn finish(&mut self) -> Option<&mut Self::Container> {
        self.builder.finish()
    }
}

impl<T, CB: PushInto<T>> PushInto<T> for FueledBuilder<CB> {
    #[inline]
    fn push_into(&mut self, item: T) {
        self.builder.push_into(item);
    }
}
