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

use std::fmt::{Display, Error, Formatter};

use timely::progress::frontier::AntichainRef;

pub trait AntichainExt {
    type Pretty: Display;

    fn pretty(&self) -> Self::Pretty;
}

impl<'a, T: Display> AntichainExt for AntichainRef<'a, T> {
    type Pretty = FrontierPrinter<Self>;

    fn pretty(&self) -> Self::Pretty {
        FrontierPrinter(*self)
    }
}

pub struct FrontierPrinter<F>(F);

impl<'a, T: Display> Display for FrontierPrinter<AntichainRef<'a, T>> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        f.write_str("{")?;
        let mut time_iter = self.0.iter();
        if let Some(t) = time_iter.next() {
            t.fmt(f)?;
        }
        for t in time_iter {
            f.write_str(", ")?;
            t.fmt(f)?;
        }
        f.write_str("}")?;
        Ok(())
    }
}
