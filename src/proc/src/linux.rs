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

//! Linux-specific process introspection.

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::ffi::{c_int, CStr, OsStr};
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;

use anyhow::Context;
use libc::{c_void, dl_iterate_phdr, dl_phdr_info, size_t, Elf64_Word, PT_NOTE};
use mz_ore::bits::align_up;
use mz_ore::cast::CastFrom;

use crate::BuildId;

/// Gets the GNU build IDs for all loaded images, including the main
/// program binary as well as all dynamically loaded libraries.
/// Intended to be useful for profilers, who can use the supplied IDs
/// to symbolize stack traces offline.
///
/// Uses `dl_iterate_phdr` to walk the program headers of all images,
/// and iterates over them looking for note
/// segments. Then searches the discovered note segments for a note of type
/// `NT_GNU_BUILD_ID` (aka "3") and name "GNU\0".
///
/// SAFETY: This function is written in a hilariously unsafe way: it involves
/// following pointers to random parts of memory, and then assuming
/// that particular structures can be found there.
/// However, it was written by carefully reading `man dl_iterate_phdr`
/// and `man elf`, and is thus intended to be relatively safe for callers to use.
/// Assuming I haven't written any bugs (and that the documentation is correct),
/// the only known safety requirements are:
///
/// (1) It must not be called multiple times concurrently, as `dl_iterate_phdr`
/// is not documented as being thread-safe.
/// (2) The running binary must be in ELF format and running on Linux.
pub unsafe fn all_build_ids(
) -> Result<std::collections::BTreeMap<std::path::PathBuf, BuildId>, anyhow::Error> {
    let mut state = CallbackState {
        map: BTreeMap::new(),
        is_first: true,
        fatal_error: None,
    };
    // SAFETY: `dl_iterate_phdr` has no documented restrictions on when
    // it can be called.
    unsafe {
        dl_iterate_phdr(Some(iterate_cb), std::ptr::addr_of_mut!(state).cast());
    };
    if let Some(err) = state.fatal_error {
        Err(err)
    } else {
        Ok(state.map)
    }
}

struct CallbackState {
    map: BTreeMap<PathBuf, BuildId>,
    is_first: bool,
    fatal_error: Option<anyhow::Error>,
}

extern "C" fn iterate_cb(info: *mut dl_phdr_info, _size: size_t, data: *mut c_void) -> c_int {
    let state: *mut CallbackState = data.cast();

    // SAFETY: `data` is a pointer to a `CallbackState`, and no mutable reference
    // aliases with it in Rust. Furthermore, `dl_iterate_phdr` doesn't do anything
    // with `data` other than pass it to this callback, so nothing will be mutating
    // the object it points to while we're inside here.
    assert_pointer_valid(state);
    let state = unsafe { state.as_mut() }.expect("pointer is valid");

    // SAFETY: similarly, `dl_iterate_phdr` isn't mutating `info`
    // while we're here.
    assert_pointer_valid(info);
    let info = unsafe { info.as_ref() }.expect("pointer is valid");

    let fname = if state.is_first {
        // From `man dl_iterate_phdr`:
        // "The first object visited by callback is the main program.  For the main
        // program, the dlpi_name field will be an empty string."
        match std::env::current_exe().context("failed to read the name of the current executable") {
            Ok(pb) => Some(pb),
            Err(e) => {
                // Profiles will be of dubious usefulness
                // if we can't get the build ID for the main executable,
                // so just bail here.
                state.fatal_error = Some(e);
                return -1;
            }
        }
    } else if info.dlpi_name.is_null() {
        None
    } else {
        // SAFETY: `dl_iterate_phdr` documents this as being a null-terminated string.
        assert_pointer_valid(info.dlpi_name);
        let fname = unsafe { CStr::from_ptr(info.dlpi_name) };

        Some(OsStr::from_bytes(fname.to_bytes()).into())
    };
    state.is_first = false;
    if let Some(fname) = fname {
        if let Entry::Vacant(ve) = state.map.entry(fname) {
            // Walk the headers of this image, looking for a segment containing notes

            // SAFETY: `dl_iterate_phdr` is documented as setting `dlpi_phnum` to the
            // length of the array pointed to by `dlpi_phdr`.
            assert_pointer_valid(info.dlpi_phdr);
            let program_headers =
                unsafe { std::slice::from_raw_parts(info.dlpi_phdr, info.dlpi_phnum.into()) };

            let mut found_build_id = None;
            'outer: for ph in program_headers {
                if ph.p_type == PT_NOTE {
                    // From `man elf`:
                    // typedef struct {
                    //   Elf64_Word n_namesz;
                    //   Elf64_Word n_descsz;
                    //   Elf64_Word n_type;
                    // } Elf64_Nhdr;
                    #[repr(C)]
                    struct NoteHeader {
                        n_namesz: Elf64_Word,
                        n_descsz: Elf64_Word,
                        n_type: Elf64_Word,
                    }
                    // This is how `man dl_iterate_phdr` says to find the segment headers in memory.
                    let mut offset = usize::cast_from(ph.p_vaddr + info.dlpi_addr);
                    let orig_offset = offset;

                    const NT_GNU_BUILD_ID: Elf64_Word = 3;
                    const GNU_NOTE_NAME: &[u8; 4] = b"GNU\0";
                    const ELF_NOTE_STRING_ALIGN: usize = 4;

                    while offset + std::mem::size_of::<NoteHeader>() + GNU_NOTE_NAME.len()
                        <= orig_offset + usize::cast_from(ph.p_memsz)
                    {
                        // Justification: Our logic for walking this header
                        // follows exactly the code snippet in the
                        // `Notes (Nhdr)` section of `man elf`,
                        // so `offset` will always point to a `NoteHeader`
                        // (called `Elf64_Nhdr` in that document)
                        #[allow(clippy::as_conversions)]
                        let nh_ptr = offset as *const NoteHeader;

                        // SAFETY: Iterating according to the `Notes (Nhdr)`
                        // section of `man elf` ensures that this pointer is
                        // aligned. The offset check above ensures that it
                        // is in-bounds.
                        assert_pointer_valid(nh_ptr);
                        let nh = unsafe { nh_ptr.as_ref() }.expect("pointer is valid");

                        // from elf.h
                        if nh.n_type == NT_GNU_BUILD_ID
                            && nh.n_descsz != 0
                            && usize::cast_from(nh.n_namesz) == GNU_NOTE_NAME.len()
                        {
                            // Justification: since `n_namesz` is 4, the name is a four-byte value.
                            #[allow(clippy::as_conversions)]
                            let p_name =
                                (offset + std::mem::size_of::<NoteHeader>()) as *const [u8; 4];

                            // SAFETY: since `n_namesz` is 4, the name is a four-byte value.
                            assert_pointer_valid(p_name);
                            let name = unsafe { p_name.as_ref() }.expect("pointer is valid");

                            if name == GNU_NOTE_NAME {
                                // We found what we're looking for!
                                // Justification: simple pointer arithmetic
                                #[allow(clippy::as_conversions)]
                                let p_desc = (p_name as usize + 4) as *const u8;

                                // SAFETY: This is the documented meaning of `n_descsz`.
                                assert_pointer_valid(p_desc);
                                let desc = unsafe {
                                    std::slice::from_raw_parts(
                                        p_desc,
                                        usize::cast_from(nh.n_descsz),
                                    )
                                };

                                found_build_id = Some(BuildId(desc.to_vec()));
                                break 'outer;
                            }
                        }
                        offset = offset
                            + std::mem::size_of::<NoteHeader>()
                            + align_up::<ELF_NOTE_STRING_ALIGN>(usize::cast_from(nh.n_namesz))
                            + align_up::<ELF_NOTE_STRING_ALIGN>(usize::cast_from(nh.n_descsz));
                    }
                }
            }
            if let Some(build_id) = found_build_id {
                ve.insert(build_id);
            }
        }
    }
    0
}

/// Asserts that the given pointer is valid.
///
/// # Panics
///
/// Panics if the given pointer:
///  * is a null pointer
///  * is not properly aligned for `T`
fn assert_pointer_valid<T>(ptr: *const T) {
    // No other known way to convert a pointer to `usize`.
    #[allow(clippy::as_conversions)]
    let address = ptr as usize;
    let align = std::mem::align_of::<T>();

    assert!(!ptr.is_null());
    assert!(address % align == 0, "unaligned pointer");
}
