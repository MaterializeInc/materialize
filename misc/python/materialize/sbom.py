# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Post-processing for the CycloneDX SBOMs produced by `inspector-sbomgen`.

sbomgen describes every crate in a `Cargo.lock` as `pkg:cargo/<name>@<version>`,
whatever its `source`. For a crate built from a git fork that is the purl of
the crates.io release with the same version number, so downstream tooling
cannot tell the fork from the upstream release. `annotate_cargo_git_sources`
records the git source on those components: a `vcs_url` purl qualifier, a `vcs`
external reference, and a pedigree naming the commit.
"""

import argparse
import json
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

SOURCE_PATH_PROPERTY = "amazon:inspector:sbom_generator:source_path"

PEDIGREE_NOTE = (
    "Built from a git repository rather than the crates.io release. The version"
    " is the one declared in that repository's Cargo.toml and does not identify"
    " a crates.io release."
)


@dataclass(frozen=True)
class GitSource:
    """A `git+` source from a `Cargo.lock` package entry."""

    repository: str
    """The repository URL, without the `git+` prefix, query or fragment."""
    commit: str
    """The commit Cargo resolved, from the URL fragment."""

    @classmethod
    def parse(cls, source: str) -> "GitSource | None":
        """Parse a lockfile `source`, or return None if it is not a git source.

        >>> GitSource.parse("git+https://github.com/o/r.git?rev=abc#0123")
        GitSource(repository='https://github.com/o/r.git', commit='0123')
        >>> GitSource.parse("registry+https://github.com/rust-lang/crates.io-index")
        """
        if not source.startswith("git+"):
            return None
        url = urlsplit(source.removeprefix("git+"))
        if not url.fragment:
            raise ValueError(f"git source without a resolved commit: {source}")
        repository = urlunsplit((url.scheme, url.netloc, url.path, "", ""))
        return cls(repository=repository, commit=url.fragment)

    def vcs_url(self) -> str:
        """The purl `vcs_url` form: `git+<transport>://<host>/<path>@<revision>`."""
        return f"git+{self.repository}@{self.commit}"


def read_lockfile(path: Path) -> dict[tuple[str, str], list[str | None]]:
    """Map each (name, version) in a `Cargo.lock` to the sources it appears with.

    Path dependencies and workspace members have no `source`; they map to None.
    """
    with path.open("rb") as f:
        lock = tomllib.load(f)
    sources: dict[tuple[str, str], list[str | None]] = {}
    for package in lock.get("package", []):
        key = (package["name"], package["version"])
        sources.setdefault(key, []).append(package.get("source"))
    return sources


def _property(component: dict[str, Any], name: str) -> str | None:
    for prop in component.get("properties", []):
        if prop.get("name") == name:
            return prop.get("value")
    return None


def _annotate(component: dict[str, Any], git: GitSource) -> None:
    vcs_url = git.vcs_url()
    # Qualifier values are percent-encoded per the purl spec.
    component["purl"] = f"{component['purl']}?vcs_url={quote(vcs_url, safe='')}"
    component.setdefault("externalReferences", []).append(
        {
            "type": "vcs",
            "url": git.repository,
            "comment": f"Built from commit {git.commit}",
        }
    )
    component["pedigree"] = {
        "commits": [{"uid": git.commit}],
        "notes": PEDIGREE_NOTE,
    }


def annotate_cargo_git_sources(
    bom: dict[str, Any], root: Path
) -> tuple[int, list[str]]:
    """Record the git source of every git-sourced Cargo component in `bom`.

    Components are matched to the lockfile sbomgen read them from (its
    `source_path` property, relative to `root`) by name and version. A
    lockfile can hold one name and version from more than one source; such a
    component cannot be attributed to either, so it is left unchanged and
    reported. Components that already carry purl qualifiers or a subpath are
    also left unchanged, which makes the annotation idempotent.

    Modifies `bom` in place. Returns the number of components annotated and a
    warning for each one skipped.
    """
    lockfiles: dict[str, dict[tuple[str, str], list[str | None]]] = {}
    annotated = 0
    warnings = []
    for component in bom.get("components", []):
        purl = component.get("purl", "")
        source_path = _property(component, SOURCE_PATH_PROPERTY)
        if not purl.startswith("pkg:cargo/") or source_path is None:
            continue
        if Path(source_path).name != "Cargo.lock":
            continue
        if source_path not in lockfiles:
            lockfiles[source_path] = read_lockfile(root / source_path)

        key = (component["name"], component["version"])
        sources = lockfiles[source_path].get(key, [])
        git_sources = [g for g in map(GitSource.parse, filter(None, sources)) if g]
        if not git_sources:
            continue
        if len(sources) > 1:
            warnings.append(
                f"{purl} ({source_path}): appears with {len(sources)} sources"
                f" ({', '.join(str(s) for s in sources)}), cannot tell which"
                " this component is; left unchanged"
            )
            continue
        if "?" in purl or "#" in purl:
            if "vcs_url=" not in purl:
                warnings.append(
                    f"{purl} ({source_path}): purl already has qualifiers or a"
                    " subpath; left unchanged"
                )
            continue
        _annotate(component, git_sources[0])
        annotated += 1
    return annotated, warnings


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="sbom",
        description="Record the git source of git-sourced crates in a CycloneDX SBOM generated by inspector-sbomgen.",
    )
    parser.add_argument("sbom", type=Path, help="SBOM to annotate in place")
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("."),
        help="directory the SBOM's source paths are relative to (default: .)",
    )
    args = parser.parse_args()

    bom = json.loads(args.sbom.read_text())
    annotated, warnings = annotate_cargo_git_sources(bom, args.root)
    for warning in warnings:
        print(f"sbom: warning: {warning}", file=sys.stderr)
    print(f"sbom: recorded the git source of {annotated} components", file=sys.stderr)
    args.sbom.write_text(json.dumps(bom, indent=2, ensure_ascii=False) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
