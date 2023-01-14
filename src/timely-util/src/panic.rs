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

use std::panic;

use mz_ore::halt;

/// Intercepts expected [`timely::communication`] panics and downgrades them to
/// [`halt`]s.
///
/// Because processes in a timely cluster are shared fate, once one process in
/// the cluster crashes, the other processes in the cluster are expected to
/// panic with communication errors. This function sniffs out these
/// communication errors and downgrades them to halts, to keep the attention on
/// the process that crashed first.
pub fn halt_on_timely_communication_panic() {
    let old_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // We have to sniff out expected panics based on their message because
        // Rust does not have good support for panicking with structured
        // payloads.
        match panic_info.payload().downcast_ref::<String>() {
            Some(e) if e.starts_with("timely communication error:") => {
                halt!("{}", e);
            }
            _ => old_hook(panic_info),
        }
    }))
}
