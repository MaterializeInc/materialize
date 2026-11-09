# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Check our set of Cargo.toml files for issues"""

import argparse
import os
import sys
from collections.abc import Callable
from pathlib import Path
from pprint import pprint

import toml

from materialize import MZ_ROOT, spawn
from materialize.cargo import Workspace


def check_rust_versions(workspace: Workspace) -> bool:
    """Checks that every crate has a minimum specified rust version, and furthermore,
    that they are all the same."""

    rust_version_to_crate_name: dict[str | None, list[str]] = {}
    for name, crate in workspace.crates.items():
        rust_version_to_crate_name.setdefault(crate.rust_version, []).append(name)
    success = (
        len(rust_version_to_crate_name) == 1 and None not in rust_version_to_crate_name
    )
    if not success:
        print(
            "Not all crates have the same rust-version value. Rust versions found:",
            file=sys.stderr,
        )
        pprint(rust_version_to_crate_name, stream=sys.stderr)
    return success


def check_default_members(workspace: Workspace) -> bool:
    """Checks that the default members for the workspace includes all crates
    except those that are intentionally excluded."""

    EXCLUDED = {
        # This crate force enables jemalloc on Linux. We want it to be excluded
        # by default, and only pulled in by feature flags for other crates. This
        # ensures that `--no-default-features` properly disables jemalloc.
        "src/alloc-default",
        "src/fivetran-destination",
    }

    crates = set(str(c.path.relative_to(c.root)) for c in workspace.crates.values())
    default_members = set(workspace.default_members)

    missing = crates - EXCLUDED - default_members
    success = len(missing) == 0
    if not success:
        print(
            "Crates missing from workspace.default-members in root Cargo.toml:",
            file=sys.stderr,
        )
        for crate in sorted(missing):
            print(f'    "{crate}",', file=sys.stderr)
    return success


def check_workspace_dependencies(workspace: Workspace) -> bool:
    """Checks that crates declare every dependency that is not a path
    dependency with `workspace = true`, so that its version and source live in
    the root `[workspace.dependencies]`."""

    success = True
    for name, crate in sorted(workspace.crates.items()):
        for dep, dep_types in crate.non_workspace_deps.items():
            print(
                f"{name}: {dep} should use `workspace = true` "
                f"(found in {', '.join(dep_types)})",
                file=sys.stderr,
            )
            success = False
    if not success:
        print(
            "\nhint: declare the above dependencies in `[workspace.dependencies]` "
            'of the root Cargo.toml if missing, and replace `dep = "version"` '
            "with `dep.workspace = true` or `dep = { workspace = true, ... }`",
            file=sys.stderr,
        )
    return success


def check_fuzz_versions_mirror_root(workspace: Workspace) -> bool:
    """Checks that the cargo-fuzz crates pin the same dependency versions as the
    root workspace.

    The fuzz crates live in their own workspace (test/cargo-fuzz) because
    libFuzzer requires nightly Rust, so they cannot use `workspace = true` to
    inherit the root `[workspace.dependencies]`. They inline versions instead,
    which silently drift from the root and make a fuzz target exercise a
    different dependency than production. Enforce that any inlined version whose
    crate is also a root workspace dependency matches the root version exactly.
    Features are intentionally not compared. Fuzz crates deliberately pull a
    minimal feature set."""

    def version_req(spec: object) -> str | None:
        # None for path/workspace/git-only specs that pin no registry version.
        if isinstance(spec, str):
            return spec
        if isinstance(spec, dict) and "path" not in spec and not spec.get("workspace"):
            version = spec.get("version")
            return version if isinstance(version, str) else None
        return None

    root_versions = {
        name: version_req(spec)
        for name, spec in workspace.workspace_dependencies.items()
    }

    fuzz_workspace = workspace.root / "test" / "cargo-fuzz"
    with open(fuzz_workspace / "Cargo.toml") as f:
        members = toml.load(f)["workspace"]["members"]

    success = True
    for member in members:
        with open(fuzz_workspace / member / "Cargo.toml") as f:
            config = toml.load(f)
        rel = os.path.normpath(os.path.join("test/cargo-fuzz", member))
        for section in ("dependencies", "build-dependencies", "dev-dependencies"):
            for name, spec in config.get(section, {}).items():
                version = version_req(spec)
                root_version = root_versions.get(name)
                if version is None or root_version is None:
                    continue
                if version != root_version:
                    print(
                        f'{rel}/Cargo.toml: {name} = "{version}" must match the '
                        f'root workspace version "{root_version}"',
                        file=sys.stderr,
                    )
                    success = False
    if not success:
        print(
            "\nhint: fuzz crates can't use `workspace = true`, so pin the exact "
            "root version. Update each value above to match the root Cargo.toml.",
            file=sys.stderr,
        )
    return success


def check_fuzz_patches_mirror_root(workspace: Workspace) -> bool:
    """Checks that the cargo-fuzz workspace (test/cargo-fuzz) carries the root
    workspace's `[patch.crates-io]` entries verbatim.

    Cargo never fails over a patch mismatch. An entry that does not apply lands
    in `[[patch.unused]]` with only a warning, and a missing entry passes with no
    warning at all. Either way the fuzz targets silently build against the
    crates.io release instead of the fork production uses, and the build breaks
    whenever the fork carries API the published crate lacks."""

    # Root patches for crates outside the fuzz crates' dependency graph. Cargo
    # would warn that they are unused, so the fuzz workspace leaves them out.
    OMITTED = {"postgres_array"}

    with open(workspace.root / "Cargo.toml") as f:
        root_patches = toml.load(f).get("patch", {}).get("crates-io", {})
    with open(workspace.root / "test" / "cargo-fuzz" / "Cargo.toml") as f:
        fuzz_patches = toml.load(f).get("patch", {}).get("crates-io", {})

    success = True
    for name, spec in sorted(fuzz_patches.items()):
        if name not in root_patches:
            print(
                f"test/cargo-fuzz/Cargo.toml: {name} is patched here but not in "
                f"the root Cargo.toml",
                file=sys.stderr,
            )
            success = False
        elif spec != root_patches[name]:
            print(
                f"test/cargo-fuzz/Cargo.toml: {name} = {spec} must match the "
                f"root Cargo.toml's {name} = {root_patches[name]}",
                file=sys.stderr,
            )
            success = False
    for name in sorted(root_patches.keys() - fuzz_patches.keys() - OMITTED):
        print(
            f"test/cargo-fuzz/Cargo.toml: {name} is patched in the root "
            f"Cargo.toml but not here",
            file=sys.stderr,
        )
        success = False
    if not success:
        print(
            "\nhint: copy the entry from the root `[patch.crates-io]` verbatim, "
            "or drop it here if the root no longer patches that crate. A root "
            "entry that no fuzz crate depends on goes into `OMITTED` in "
            "check_fuzz_patches_mirror_root instead.",
            file=sys.stderr,
        )
    return success


APACHE = "Apache-2.0"
BSL = "BUSL-1.1"
# Files carry the Apache license header when it contains this text. The BSL
# header names the Apache License as its change license, so matching on
# "Apache License" alone misclassifies BSL files.
APACHE_HEADER_MARKER = "Licensed under the Apache License, Version 2.0"
BSL_HEADER_MARKER = "Business Source License"
COPYRIGHT_MARKER = "Copyright Materialize, Inc. and contributors."

# BSL-headed files inside Apache crates, pending a decision on relabeling them
# Apache. Relabeling loosens their license, so it needs sign-off. Remove an
# entry once its file is fixed. A stale entry fails the lint.
LICENSE_ALLOWED_BSL_FILES: set[str] = {
    "src/ore-proc/tests/instrument.rs",
    "src/ore/benches/bytes.rs",
    "src/ore/benches/id_gen.rs",
    "src/sql-lexer/tests/testdata/strings",
    "src/sql-parser/src/ast/item_refs.rs",
    "src/sql-parser/tests/keyword_audit.rs",
    "src/timely-util/benches/chunk_pool.rs",
    "src/timely-util/benches/column_borrow.rs",
    "src/timely-util/benches/column_pager.rs",
    "src/timely-util/benches/columnar_merge_batcher.rs",
    "src/timely-util/benches/columnar_merger.rs",
    "src/timely-util/benches/consolidating_column_builder.rs",
    "src/timely-util/benches/reclock.rs",
    "src/timely-util/examples/column_paged_spill.rs",
    "src/timely-util/src/columnar/chunk.rs",
    "src/timely-util/src/columnar/chunk/metrics.rs",
    "src/timely-util/src/columnar/unload.rs",
    "src/timely-util/src/containers/heap_size.rs",
    "src/timely-util/src/funded_spine.rs",
    "src/timely-util/src/funded_spine/spine_fueled.rs",
    "src/timely-util/src/funded_spine/tests.rs",
}

# Apache crates that depend on BSL crates, as (crate dir, dependency dir),
# pending the same decision.
LICENSE_ALLOWED_BSL_DEPS: set[tuple[str, str]] = {
    ("src/prof", "src/build-tools"),
    ("src/sql-parser", "src/walkabout"),
}


def _read_header(path: Path) -> str:
    try:
        with open(path, errors="replace") as f:
            return f.read(4096)
    except (IsADirectoryError, FileNotFoundError):
        return ""


def _header_license(header: str) -> str | None:
    if APACHE_HEADER_MARKER in header:
        return APACHE
    if BSL_HEADER_MARKER in header:
        return BSL
    return None


def _workspace_license(root: Path, crate_dir: str) -> str | None:
    """Returns `workspace.package.license` of the workspace enclosing
    `crate_dir`, for crates that declare `license.workspace = true`."""
    d = crate_dir
    while True:
        manifest = root / d / "Cargo.toml"
        if manifest.exists():
            with open(manifest) as f:
                config = toml.load(f)
            if "workspace" in config:
                return config["workspace"].get("package", {}).get("license")
        if d == "":
            return None
        d = os.path.dirname(d)


def check_crate_licenses(workspace: Workspace) -> bool:
    """Checks that every crate declares its license, that Apache crates
    contain no BSL files and have no BSL normal or build dependencies, and that
    every file with a Materialize copyright line names its license.

    A crate declares its license with `package.license`, either `Apache-2.0` or
    `BUSL-1.1`. The check covers every crate in the repository, including
    crates outside the root workspace such as fuzz targets. A file belongs to
    the innermost crate whose directory contains it. Dev dependencies may be
    BSL, they do not end up in the crate's artifacts."""

    root = workspace.root
    tracked = spawn.capture(["git", "ls-files"], cwd=root).splitlines()

    # None marks a crate with a missing or invalid license, already reported.
    licenses: dict[str, str | None] = {}
    configs: dict[str, dict] = {}
    success = True
    for path in tracked:
        if not path.endswith("Cargo.toml"):
            continue
        with open(root / path) as f:
            config = toml.load(f)
        if "package" not in config:
            continue
        crate_dir = os.path.dirname(path)
        license = config["package"].get("license")
        if isinstance(license, dict) and license.get("workspace"):
            license = _workspace_license(root, crate_dir)
        configs[crate_dir] = config
        if license is None:
            print(
                f'{path}: missing `license`, set it to "{APACHE}" or "{BSL}"',
                file=sys.stderr,
            )
            success = False
            licenses[crate_dir] = None
        elif not isinstance(license, str) or license not in (APACHE, BSL):
            print(
                f'{path}: license = "{license}" must be "{APACHE}" or "{BSL}"',
                file=sys.stderr,
            )
            success = False
            licenses[crate_dir] = None
        else:
            licenses[crate_dir] = license

    def owning_crate(path: str) -> str | None:
        d = os.path.dirname(path)
        while True:
            if d in licenses:
                return d
            if d == "":
                return None
            d = os.path.dirname(d)

    seen_files = set()
    for path in tracked:
        crate_dir = owning_crate(path)
        if crate_dir is None or path.endswith("Cargo.toml"):
            continue
        header = _read_header(root / path)
        header_license = _header_license(header)
        if header_license is None:
            if COPYRIGHT_MARKER in header:
                print(
                    f"{path}: copyright header names neither the {APACHE} nor "
                    f"the {BSL} license",
                    file=sys.stderr,
                )
                success = False
            continue
        # BSL crates may contain Apache files, for example code derived from
        # Apache-licensed third-party projects, which must keep its license.
        if licenses[crate_dir] != APACHE or header_license != BSL:
            continue
        if path in LICENSE_ALLOWED_BSL_FILES:
            seen_files.add(path)
            continue
        print(
            f"{path}: has a {BSL} header, but crate {crate_dir} is {APACHE}",
            file=sys.stderr,
        )
        success = False

    workspace_paths = {
        name: spec["path"]
        for name, spec in workspace.workspace_dependencies.items()
        if isinstance(spec, dict) and "path" in spec
    }
    seen_deps = set()
    for crate_dir, config in sorted(configs.items()):
        if licenses[crate_dir] != APACHE:
            continue
        sections = [config]
        sections.extend(config.get("target", {}).values())
        for section in sections:
            for dep_type in ("dependencies", "build-dependencies"):
                for name, spec in section.get(dep_type, {}).items():
                    if not isinstance(spec, dict):
                        continue
                    if "path" in spec:
                        dep_dir = os.path.normpath(
                            os.path.join(crate_dir, spec["path"])
                        )
                    elif spec.get("workspace") and name in workspace_paths:
                        dep_dir = os.path.normpath(workspace_paths[name])
                    else:
                        continue
                    if licenses.get(dep_dir) == APACHE:
                        continue
                    if (crate_dir, dep_dir) in LICENSE_ALLOWED_BSL_DEPS:
                        seen_deps.add((crate_dir, dep_dir))
                        continue
                    print(
                        f"{crate_dir}/Cargo.toml: {APACHE} crate depends on "
                        f"{BSL} crate {dep_dir} (in {dep_type})",
                        file=sys.stderr,
                    )
                    success = False

    for path in sorted(LICENSE_ALLOWED_BSL_FILES - seen_files):
        print(
            f"{path}: stale entry in LICENSE_ALLOWED_BSL_FILES, remove it",
            file=sys.stderr,
        )
        success = False
    for crate_dir, dep_dir in sorted(LICENSE_ALLOWED_BSL_DEPS - seen_deps):
        print(
            f"({crate_dir}, {dep_dir}): stale entry in "
            "LICENSE_ALLOWED_BSL_DEPS, remove it",
            file=sys.stderr,
        )
        success = False

    if not success:
        print(
            '\nhint: every crate sets `license` to "Apache-2.0" or "BUSL-1.1". '
            "Files carry the license header of their crate, copy it from the "
            "crate's `src/lib.rs`. BSL crates may also contain Apache files. "
            "An Apache crate may only depend on Apache crates outside "
            "dev-dependencies.",
            file=sys.stderr,
        )
    return success


LINTS: dict[str, Callable[[Workspace], bool]] = {
    "rust-versions": check_rust_versions,
    "default-members": check_default_members,
    "workspace-dependencies": check_workspace_dependencies,
    "fuzz-versions-mirror-root": check_fuzz_versions_mirror_root,
    "fuzz-patches-mirror-root": check_fuzz_patches_mirror_root,
    "crate-licenses": check_crate_licenses,
}


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="lint-cargo", description="Check Cargo.toml files for issues."
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=MZ_ROOT,
        help="root of the Cargo workspace to check (default: the Materialize repository)",
    )
    parser.add_argument(
        "--lint",
        action="append",
        choices=LINTS.keys(),
        help="run only this lint; repeat to run several (default: all)",
    )
    args = parser.parse_args()

    workspace = Workspace(args.root)
    lints = [LINTS[name] for name in args.lint or LINTS]
    # Run every lint, then combine. `success and lint(...)` would short-circuit
    # and skip the remaining lints after the first failure, under-reporting.
    success = all([lint(workspace) for lint in lints])
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
