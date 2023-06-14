from elftools.elf.elffile import ELFFile, NoteSection

from materialize.ui import UIError


def get_build_id(file) -> str:
    elf_file = ELFFile(file)

    build_id_section = elf_file.get_section_by_name(".note.gnu.build-id")

    if not build_id_section:
        raise UIError(f"ELF file has no .note.gnu.build-id section: {file}")

    if not isinstance(build_id_section, NoteSection):
        raise UIError(f"ELF file .note.gnu.build-id section could not be read: {file}")

    for note in build_id_section.iter_notes():
        if note.n_type == "NT_GNU_BUILD_ID" and note.n_name == "GNU":
            return note.n_desc

    raise UIError(f"ELF file GNU build ID could not be found: {file}")
