# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import BinaryIO

# `stubgen -p elftools` doesn't work out of the box,
# and manually writing stub files for all of `elftools`
# seems like a large and error-prone project.
#
# If we start using pyelftools from more places, it might be useful to
# have a stub file. A sketch of one can be found here:
# https://github.com/MaterializeInc/materialize/pull/19960#discussion_r1231516060
from elftools.elf.elffile import ELFFile, NoteSection  # type: ignore

from materialize.ui import UIError


def get_build_id(file: BinaryIO) -> str:
    elf_file = ELFFile(file)

    build_id_section = elf_file.get_section_by_name(".note.gnu.build-id")

    if not build_id_section:
        raise UIError(f"ELF file has no .note.gnu.build-id section: {file}")

    if not isinstance(build_id_section, NoteSection):
        raise UIError(f"ELF file .note.gnu.build-id section could not be read: {file}")

    for note in build_id_section.iter_notes():
        if note.n_type == "NT_GNU_BUILD_ID" and note.n_name == "GNU":
            return str(note.n_desc)

    raise UIError(f"ELF file GNU build ID could not be found: {file}")
