# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Detect OpenSSL usage across the Materialize codebase.

This is an optional linter intended to track progress toward replacing OpenSSL
with rustls. It identifies three categories of OpenSSL usage:

1. Direct Cargo.toml dependencies on OpenSSL-related packages
2. Feature flags on third-party deps that pull in OpenSSL
3. Rust source code imports of OpenSSL modules
"""

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

import toml

from materialize import MZ_ROOT
from materialize.cargo import Workspace

# Crate names that are part of the OpenSSL ecosystem.
OPENSSL_PACKAGES = frozenset(
    {
        "openssl",
        "openssl-sys",
        "openssl-probe",
        "native-tls",
        "tokio-native-tls",
        "postgres-openssl",
        "tokio-openssl",
        "hyper-openssl",
        "hyper-tls",
    }
)

# Feature flags on third-party dependencies that pull in OpenSSL.
OPENSSL_FEATURES = frozenset(
    {
        "native-tls",
        "native-tls-vendored",
        "openssl-tls",
        "default-tls",
    }
)

# Regex matching Rust imports/paths referencing OpenSSL modules.
SOURCE_PATTERN = re.compile(
    r"\b(openssl|native_tls|tokio_openssl|hyper_openssl|postgres_openssl)::"
)


@dataclass
class Finding:
    crate_name: str
    crate_path: str
    category: str  # "dependency" | "feature-flag" | "source-code"
    description: str
    file: str | None = None
    line: int | None = None


def scan_cargo_toml(crate_name: str, crate_path: Path) -> list[Finding]:
    """Scan a crate's Cargo.toml for OpenSSL-related dependencies and features."""
    findings: list[Finding] = []
    cargo_toml = crate_path / "Cargo.toml"
    if not cargo_toml.exists():
        return findings

    with open(cargo_toml) as f:
        config = toml.load(f)

    rel_path = str(crate_path.relative_to(MZ_ROOT))

    for dep_section in ("dependencies", "dev-dependencies", "build-dependencies"):
        deps = config.get(dep_section, {})
        for dep_name, dep_spec in deps.items():
            actual_name = dep_name
            features: list[str] = []

            if isinstance(dep_spec, dict):
                actual_name = dep_spec.get("package", dep_name)
                features = dep_spec.get("features", [])
            elif isinstance(dep_spec, str):
                actual_name = dep_name

            # Check if the dependency itself is an OpenSSL package.
            if actual_name in OPENSSL_PACKAGES:
                if isinstance(dep_spec, dict):
                    version = dep_spec.get("version", "?")
                else:
                    version = dep_spec
                findings.append(
                    Finding(
                        crate_name=crate_name,
                        crate_path=rel_path,
                        category="dependency",
                        description=f'{actual_name} = "{version}" (in {dep_section})',
                    )
                )

            # Check if the dependency uses features that pull in OpenSSL.
            for feature in features:
                if feature in OPENSSL_FEATURES:
                    findings.append(
                        Finding(
                            crate_name=crate_name,
                            crate_path=rel_path,
                            category="feature-flag",
                            description=f'{dep_name}: features include "{feature}" (in {dep_section})',
                        )
                    )

    # Check the crate's own [features] table for references to OpenSSL deps.
    for feature_name, feature_deps in config.get("features", {}).items():
        for fdep in feature_deps:
            if any(pkg in fdep for pkg in OPENSSL_PACKAGES):
                findings.append(
                    Finding(
                        crate_name=crate_name,
                        crate_path=rel_path,
                        category="feature-flag",
                        description=f'feature "{feature_name}" references "{fdep}"',
                    )
                )

    return findings


def scan_source_files(crate_name: str, crate_path: Path) -> list[Finding]:
    """Scan Rust source files for OpenSSL imports."""
    findings: list[Finding] = []
    rel_crate = str(crate_path.relative_to(MZ_ROOT))

    for rs_file in sorted(crate_path.rglob("*.rs")):
        try:
            lines = rs_file.read_text().splitlines()
        except (OSError, UnicodeDecodeError):
            continue

        for line_num, line in enumerate(lines, start=1):
            stripped = line.strip()
            # Skip comments.
            if stripped.startswith("//") or stripped.startswith("/*"):
                continue
            if SOURCE_PATTERN.search(line):
                rel_file = str(rs_file.relative_to(MZ_ROOT))
                findings.append(
                    Finding(
                        crate_name=crate_name,
                        crate_path=rel_crate,
                        category="source-code",
                        description=stripped,
                        file=rel_file,
                        line=line_num,
                    )
                )

    return findings


def run_scan() -> list[Finding]:
    """Scan the entire workspace for OpenSSL usage."""
    workspace = Workspace(MZ_ROOT)
    all_findings: list[Finding] = []

    for crate in sorted(workspace.crates.values(), key=lambda c: c.name):
        all_findings.extend(scan_cargo_toml(crate.name, crate.path))
        all_findings.extend(scan_source_files(crate.name, crate.path))

    return all_findings


def print_report(findings: list[Finding]) -> None:
    """Print a human-readable report grouped by crate."""
    print("=== OpenSSL Usage Report ===")
    print()

    # Group by crate.
    crates: dict[str, list[Finding]] = {}
    for f in findings:
        crates.setdefault(f.crate_name, []).append(f)

    for crate_name in sorted(crates):
        crate_findings = crates[crate_name]
        crate_path = crate_findings[0].crate_path
        print(f"--- {crate_name} ({crate_path}) ---")
        for f in crate_findings:
            if f.file and f.line:
                print(f"  [{f.category}] {f.file}:{f.line}  {f.description}")
            else:
                print(f"  [{f.category}] {f.description}")
        print()

    # Summary.
    crates_with_deps = len(
        {f.crate_name for f in findings if f.category == "dependency"}
    )
    crates_with_features = len(
        {f.crate_name for f in findings if f.category == "feature-flag"}
    )
    crates_with_source = len(
        {f.crate_name for f in findings if f.category == "source-code"}
    )

    print("=== Summary ===")
    print(f"Crates with findings: {len(crates)}")
    print(f"  dependency: {crates_with_deps}")
    print(f"  feature-flag: {crates_with_features}")
    print(f"  source-code: {crates_with_source}")
    print(f"Total findings: {len(findings)}")


def print_json(findings: list[Finding]) -> None:
    """Print findings as JSON."""
    print(json.dumps([asdict(f) for f in findings], indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="lint-openssl",
        description="Detect OpenSSL usage across the Materialize codebase.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit with code 1 if any OpenSSL usage is found.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output findings as JSON.",
    )
    args = parser.parse_args()

    findings = run_scan()

    if args.json:
        print_json(findings)
    else:
        print_report(findings)

    if args.strict and findings:
        sys.exit(1)


if __name__ == "__main__":
    main()
