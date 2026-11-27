# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import copy
from pathlib import Path
from typing import Any

from materialize.sbom import annotate_cargo_git_sources

REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"
FORK = "git+https://github.com/example/fork?rev=0123abc#0123abcdef"
OTHER_FORK = "git+https://github.com/example/other.git?branch=main#89abcdef"

LOCKFILE = f"""
version = 4

[[package]]
name = "forked"
version = "1.2.3"
source = "{FORK}"

[[package]]
name = "build-metadata"
version = "4.3.0+2.16.0"
source = "{OTHER_FORK}"

[[package]]
name = "upstream"
version = "0.1.0"
source = "{REGISTRY}"

[[package]]
name = "member"
version = "0.0.0"

[[package]]
name = "twice"
version = "2.0.0"
source = "{REGISTRY}"

[[package]]
name = "twice"
version = "2.0.0"
source = "{FORK}"
"""

# A second lockfile in which `forked` comes from crates.io.
OTHER_LOCKFILE = f"""
version = 4

[[package]]
name = "forked"
version = "1.2.3"
source = "{REGISTRY}"
"""


def component(name: str, version: str, purl_version: str, source_path: str) -> Any:
    return {
        "type": "library",
        "name": name,
        "version": version,
        "purl": f"pkg:cargo/{name}@{purl_version}",
        "properties": [
            {
                "name": "amazon:inspector:sbom_generator:source_path",
                "value": source_path,
            }
        ],
    }


def bom() -> Any:
    return {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "components": [
            component("forked", "1.2.3", "1.2.3", "Cargo.lock"),
            component("build-metadata", "4.3.0+2.16.0", "4.3.0%2B2.16.0", "Cargo.lock"),
            component("upstream", "0.1.0", "0.1.0", "Cargo.lock"),
            component("member", "0.0.0", "0.0.0", "member/Cargo.toml"),
            component("twice", "2.0.0", "2.0.0", "Cargo.lock"),
            component("forked", "1.2.3", "1.2.3", "other/Cargo.lock"),
            {"type": "application", "name": "Dockerfile"},
        ],
    }


def write_lockfiles(root: Path) -> None:
    (root / "Cargo.lock").write_text(LOCKFILE)
    (root / "other").mkdir()
    (root / "other" / "Cargo.lock").write_text(OTHER_LOCKFILE)


def test_annotates_git_sourced_crates(tmp_path: Path) -> None:
    write_lockfiles(tmp_path)
    b = bom()
    annotated, _ = annotate_cargo_git_sources(b, tmp_path)
    assert annotated == 2
    forked, build_metadata = b["components"][:2]

    assert forked["purl"] == (
        "pkg:cargo/forked@1.2.3"
        "?vcs_url=git%2Bhttps%3A%2F%2Fgithub.com%2Fexample%2Ffork%400123abcdef"
    )
    assert forked["externalReferences"] == [
        {
            "type": "vcs",
            "url": "https://github.com/example/fork",
            "comment": "Built from commit 0123abcdef",
        }
    ]
    assert forked["pedigree"]["commits"] == [{"uid": "0123abcdef"}]

    assert build_metadata["purl"] == (
        "pkg:cargo/build-metadata@4.3.0%2B2.16.0"
        "?vcs_url=git%2Bhttps%3A%2F%2Fgithub.com%2Fexample%2Fother.git%4089abcdef"
    )


def test_leaves_other_components_unchanged(tmp_path: Path) -> None:
    write_lockfiles(tmp_path)
    b = bom()
    annotate_cargo_git_sources(b, tmp_path)
    # Registry crates, workspace members, non-Cargo components, and a crate
    # whose name and version is git-sourced only in a different lockfile.
    for i in (2, 3, 5, 6):
        assert b["components"][i] == bom()["components"][i]


def test_ambiguous_name_and_version_is_reported_not_guessed(tmp_path: Path) -> None:
    write_lockfiles(tmp_path)
    b = bom()
    _, warnings = annotate_cargo_git_sources(b, tmp_path)
    assert b["components"][4] == bom()["components"][4]
    assert len(warnings) == 1
    assert warnings[0].startswith("pkg:cargo/twice@2.0.0 (Cargo.lock): appears with 2")


def test_idempotent(tmp_path: Path) -> None:
    write_lockfiles(tmp_path)
    b = bom()
    annotate_cargo_git_sources(b, tmp_path)
    once = copy.deepcopy(b)
    annotated, warnings = annotate_cargo_git_sources(b, tmp_path)
    assert b == once
    assert annotated == 0
    assert len(warnings) == 1  # `twice`, as on the first run
