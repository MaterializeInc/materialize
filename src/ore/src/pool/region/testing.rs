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

//! Test-only accessors of the region allocator.

// The file-mode and file-store tests are Linux-only, so their helpers are
// unused in non-Linux test builds.
#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use super::*;

impl SlotAllocator {
    /// Test hook: the number of allocated slots not on a free list.
    pub(crate) fn in_use(&self) -> usize {
        usize::cast_from(self.high_water) - self.free_warm.len() - self.free_cold.len()
    }
}

impl Region {
    /// Test hook: the number of slots currently allocated.
    pub(crate) fn slots_in_use(&self) -> usize {
        self.slots
            .lock()
            .expect("region allocator poisoned")
            .in_use()
    }

    /// Test hook: overwrites every free slot with `0xDE` so stale contents
    /// cannot masquerade as correct data when a slot is reused.
    pub(crate) fn poison_free_slots(&self) {
        let slots = self.slots.lock().expect("region allocator poisoned");
        for &slot in slots.free_warm.iter().chain(slots.free_cold.iter()) {
            let offset = usize::cast_from(slot) * self.class_size;
            // SAFETY: the slot is on a free list and the allocator mutex is
            // held, so no chunk owns it and no allocation can race; the write
            // stays within the region's mapping.
            unsafe {
                std::ptr::write_bytes(self.base.add(offset), 0xDE, self.class_size);
            }
        }
    }
}
