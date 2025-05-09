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

//! A chunked columnar container based on the columnation library. It stores the local
//! portion in region-allocated data, too, which is different to the `TimelyStack` type.

use std::cell::Cell;

use differential_dataflow::containers::{Columnation, TimelyStack};
use timely::container::{ContainerBuilder, PushInto};

/// A Stacked container builder that keep track of container memory usage.
#[derive(Default)]
pub struct AccountedStackBuilder<CB> {
    pub bytes: Cell<usize>,
    pub builder: CB,
}

impl<T, CB> ContainerBuilder for AccountedStackBuilder<CB>
where
    T: Clone + Columnation + 'static,
    CB: ContainerBuilder<Container = TimelyStack<T>>,
{
    type Container = TimelyStack<T>;

    fn extract(&mut self) -> Option<&mut Self::Container> {
        let container = self.builder.extract()?;
        let mut new_bytes = 0;
        container.heap_size(|_, cap| new_bytes += cap);
        self.bytes.set(self.bytes.get() + new_bytes);
        Some(container)
    }

    fn finish(&mut self) -> Option<&mut Self::Container> {
        let container = self.builder.finish()?;
        let mut new_bytes = 0;
        container.heap_size(|_, cap| new_bytes += cap);
        self.bytes.set(self.bytes.get() + new_bytes);
        Some(container)
    }
}

impl<T, CB: PushInto<T>> PushInto<T> for AccountedStackBuilder<CB> {
    #[inline]
    fn push_into(&mut self, item: T) {
        self.builder.push_into(item);
    }
}
