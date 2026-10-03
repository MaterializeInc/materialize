# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""A minimal reader for SCIP code intelligence indexes.

Decodes only the fields needed to build a reference graph: document paths, and
for each occurrence its symbol, roles, and enclosing range. The protobuf wire
format is decoded by hand to avoid a protobuf dependency. Field numbers follow
https://github.com/sourcegraph/scip/blob/main/scip.proto.
"""

from collections.abc import Iterator
from dataclasses import dataclass, field

# `SymbolRole` bit flags.
ROLE_DEFINITION = 0x1
ROLE_IMPORT = 0x2


@dataclass
class Occurrence:
    symbol: str
    roles: int
    # `[start_line, start_char, end_line, end_char]`, or three elements when
    # the range is single-line (`[line, start_char, end_char]`).
    range: list[int]
    # Range of the nearest enclosing AST node, empty if the indexer omits it.
    enclosing_range: list[int]


@dataclass
class Document:
    relative_path: str
    occurrences: list[Occurrence] = field(default_factory=list)


def _varint(buf: bytes, pos: int) -> tuple[int, int]:
    result = 0
    shift = 0
    while True:
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if b < 0x80:
            return result, pos
        shift += 7


def _fields(buf: bytes, pos: int, end: int) -> Iterator[tuple[int, int, int, int]]:
    """Yields `(field_number, wire_type, value_or_start, end)` for each field.

    For varints, the third element is the value. For length-delimited fields,
    the third and fourth elements bound the payload. Fixed-width fields are
    skipped over and reported with their byte bounds.
    """
    while pos < end:
        key, pos = _varint(buf, pos)
        number, wire = key >> 3, key & 7
        if wire == 0:
            value, pos = _varint(buf, pos)
            yield number, wire, value, pos
        elif wire == 2:
            length, pos = _varint(buf, pos)
            yield number, wire, pos, pos + length
            pos += length
        elif wire == 1:
            yield number, wire, pos, pos + 8
            pos += 8
        elif wire == 5:
            yield number, wire, pos, pos + 4
            pos += 4
        else:
            raise ValueError(f"unsupported protobuf wire type {wire} at {pos}")


def _packed_int32(buf: bytes, start: int, end: int) -> list[int]:
    out = []
    pos = start
    while pos < end:
        value, pos = _varint(buf, pos)
        out.append(value)
    return out


def _occurrence(buf: bytes, start: int, end: int) -> Occurrence:
    symbol = ""
    roles = 0
    range_: list[int] = []
    enclosing: list[int] = []
    for number, wire, a, b in _fields(buf, start, end):
        if number == 1:
            range_ = _packed_int32(buf, a, b) if wire == 2 else [a]
        elif number == 2:
            symbol = buf[a:b].decode()
        elif number == 3:
            roles = a
        elif number == 7:
            enclosing = _packed_int32(buf, a, b) if wire == 2 else [a]
    return Occurrence(symbol, roles, range_, enclosing)


def _document(buf: bytes, start: int, end: int) -> Document:
    doc = Document("")
    for number, _wire, a, b in _fields(buf, start, end):
        if number == 1:
            doc.relative_path = buf[a:b].decode()
        elif number == 2:
            doc.occurrences.append(_occurrence(buf, a, b))
    return doc


def read_documents(buf: bytes) -> Iterator[Document]:
    """Yields every document of a serialized `scip.Index`."""
    for number, _wire, a, b in _fields(buf, 0, len(buf)):
        if number == 2:
            yield _document(buf, a, b)


@dataclass(frozen=True)
class Symbol:
    """A parsed global SCIP symbol, `<scheme> <manager> <package> <version> <descriptors>`."""

    package: str
    version: str
    descriptors: str

    @property
    def is_module(self) -> bool:
        """Whether the symbol names a module (a namespace descriptor)."""
        return self.descriptors.endswith("/")

    def type_name(self) -> str | None:
        """The name of a type or trait defined directly in a module, else `None`."""
        last = self.descriptors.rsplit("/", 1)[-1]
        if last.endswith("#") and last.count("#") == 1 and "[" not in last:
            return last[:-1].strip("`")
        return None

    def impl_header(self) -> tuple[str, str | None] | None:
        """The base names of `(self type, trait)` for an item of an impl block.

        rust-analyzer names impl items `<module>/impl#[SelfType][Trait]item`,
        with the trait omitted for inherent impls. Generic arguments and
        reference sigils are stripped, so `&'a Foo<T>` becomes `Foo`.
        """
        last = self.descriptors.rsplit("/", 1)[-1]
        if not last.startswith("impl#["):
            return None
        names = _bracketed(last[len("impl#") :])
        if not names:
            return None
        trait = _base_name(names[1]) if len(names) > 1 else None
        return _base_name(names[0]), trait


def _bracketed(s: str) -> list[str]:
    """Splits leading `[...]` groups, honoring nested brackets and backticks."""
    out = []
    pos = 0
    while pos < len(s) and s[pos] == "[":
        depth = 0
        quoted = False
        for i in range(pos, len(s)):
            c = s[i]
            if c == "`":
                quoted = not quoted
            elif not quoted and c == "[":
                depth += 1
            elif not quoted and c == "]":
                depth -= 1
                if depth == 0:
                    out.append(s[pos + 1 : i])
                    pos = i + 1
                    break
        else:
            break
    return out


def _base_name(ty: str) -> str:
    ty = ty.strip("`").lstrip("&*")
    for prefix in ("mut ", "const ", "dyn ", "impl "):
        ty = ty.removeprefix(prefix)
    if ty.startswith("'"):
        ty = ty.split(" ", 1)[-1]
    ty = ty.split("<", 1)[0]
    return ty.rsplit("::", 1)[-1].strip()


def parse_symbol(symbol: str) -> Symbol | None:
    """Parses a global symbol, returning `None` for local symbols."""
    if symbol.startswith("local "):
        return None
    # Spaces inside a field are escaped as double spaces. Package names and
    # versions in Cargo indexes never contain spaces, so a plain split of the
    # first four fields is exact.
    parts = symbol.split(" ", 4)
    if len(parts) != 5:
        return None
    _scheme, _manager, package, version, descriptors = parts
    return Symbol(package, version, descriptors)
