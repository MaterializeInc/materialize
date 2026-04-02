# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Audit container definitions for FIPS 140-3 compliance gaps.

Scans Dockerfiles for base images, crypto-relevant packages, and TLS
configuration that may not meet FIPS requirements. Distinguishes between
production images (which must be FIPS-compliant) and test/dev images
(informational only).
"""

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

from materialize import MZ_ROOT

# Production image Dockerfiles — these MUST be FIPS-compliant.
# Identified by their base image role in the MZFROM dependency chain.
PRODUCTION_DOCKERFILES = {
    "misc/images/ubuntu-base/Dockerfile",
    "misc/images/prod-base/Dockerfile",
    "misc/images/distroless-prod-base/Dockerfile",
    "misc/images/materialized-base/Dockerfile",
    "src/materialized/ci/Dockerfile",
    "src/clusterd/ci/Dockerfile",
    "src/environmentd/ci/Dockerfile",
    "src/balancerd/ci/Dockerfile",
    "src/orchestratord/ci/Dockerfile",
    "src/fivetran-destination/ci/Dockerfile",
    "misc/images/mz/Dockerfile",
    "misc/images/jobs/Dockerfile",
}

# Base images known to NOT be FIPS-validated.
NON_FIPS_BASE_IMAGES = [
    re.compile(r"^FROM\s+ubuntu:"),
    re.compile(r"^FROM\s+debian:"),
    re.compile(r"^FROM\s+alpine:"),
    re.compile(r"^FROM\s+node:"),
    re.compile(r"^FROM\s+python:"),
    re.compile(r"^FROM\s+golang:"),
    re.compile(r"^FROM\s+gcr\.io/distroless/"),
    re.compile(r"^FROM\s+postgres:"),
    re.compile(r"^FROM\s+mysql:"),
    re.compile(r"^FROM\s+nginxinc/"),
]

# Packages that bring in or use crypto, and should be audited for FIPS.
CRYPTO_PACKAGES = {
    "openssl": "Links against system OpenSSL — must be FIPS-validated in FIPS mode",
    "libssl-dev": "OpenSSL development headers — system OpenSSL must be FIPS-validated",
    "curl": "Uses system TLS (OpenSSL) for HTTPS connections",
    "wget": "Uses system TLS (OpenSSL or GnuTLS) for HTTPS connections",
    "openssh-client": "Uses system crypto for SSH connections",
    "openssh-server": "Uses system crypto for SSH connections",
    "gnupg2": "Uses its own crypto implementation (libgcrypt)",
    "gnupg": "Uses its own crypto implementation (libgcrypt)",
}

# Packages that embed their own TLS/crypto stacks.
EMBEDDED_CRYPTO_PACKAGES = {
    "postgresql-16": "PostgreSQL links against system OpenSSL for TLS",
    "postgresql-17": "PostgreSQL links against system OpenSSL for TLS",
    "nginx": "Nginx links against system OpenSSL for TLS termination",
}

# Patterns in cert generation scripts that indicate non-FIPS algorithms.
NON_FIPS_CERT_PATTERNS = [
    (
        re.compile(r"-md5\b|md5WithRSAEncryption"),
        "MD5 is not FIPS-approved for signatures",
    ),
    (
        re.compile(r"-sha1\b|sha1WithRSAEncryption"),
        "SHA-1 is not FIPS-approved for signatures (after 2030)",
    ),
    (re.compile(r"rsa:1024\b|rsa:512\b"), "RSA key size < 2048 is not FIPS-approved"),
    (
        re.compile(r"des-cbc\b|des-ede\b|rc4\b|rc2\b"),
        "DES/RC4/RC2 are not FIPS-approved",
    ),
]


@dataclass
class Finding:
    file: str
    line: int | None
    category: str  # "base-image" | "crypto-package" | "cert-generation" | "missing-fips-config"
    severity: str  # "production" | "test"
    description: str


def classify_dockerfile(rel_path: str) -> str:
    """Classify a Dockerfile as production or test."""
    if rel_path in PRODUCTION_DOCKERFILES:
        return "production"
    return "test"


def scan_dockerfile(path: Path) -> list[Finding]:
    """Scan a single Dockerfile for FIPS compliance issues."""
    findings: list[Finding] = []
    rel_path = str(path.relative_to(MZ_ROOT))
    severity = classify_dockerfile(rel_path)

    try:
        content = path.read_text()
        lines = content.splitlines()
    except (OSError, UnicodeDecodeError):
        return findings

    for line_num, line in enumerate(lines, start=1):
        stripped = line.strip()

        # Skip comments and empty lines.
        if stripped.startswith("#") or not stripped:
            continue

        # Check base images.
        for pattern in NON_FIPS_BASE_IMAGES:
            if pattern.search(stripped):
                findings.append(
                    Finding(
                        file=rel_path,
                        line=line_num,
                        category="base-image",
                        severity=severity,
                        description=f"Non-FIPS base image: {stripped}",
                    )
                )

    # Join continuation lines (backslash + newline) to detect packages in
    # multiline apt-get install commands.
    joined = content.replace("\\\n", " ")
    for joined_line in joined.splitlines():
        stripped = joined_line.strip()
        if "apt-get" in stripped or "apk add" in stripped or "yum install" in stripped:
            for pkg, reason in {**CRYPTO_PACKAGES, **EMBEDDED_CRYPTO_PACKAGES}.items():
                if re.search(rf"\b{re.escape(pkg)}\b", stripped):
                    # Find the original line number by searching for the package.
                    pkg_line = None
                    for ln, orig in enumerate(lines, start=1):
                        if pkg in orig:
                            pkg_line = ln
                            break
                    findings.append(
                        Finding(
                            file=rel_path,
                            line=pkg_line,
                            category="crypto-package",
                            severity=severity,
                            description=f"Installs {pkg}: {reason}",
                        )
                    )

    return findings


def scan_cert_scripts(path: Path) -> list[Finding]:
    """Scan certificate generation scripts for non-FIPS algorithms."""
    findings: list[Finding] = []
    rel_path = str(path.relative_to(MZ_ROOT))

    try:
        lines = path.read_text().splitlines()
    except (OSError, UnicodeDecodeError):
        return findings

    for line_num, line in enumerate(lines, start=1):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue

        for pattern, reason in NON_FIPS_CERT_PATTERNS:
            if pattern.search(stripped):
                findings.append(
                    Finding(
                        file=rel_path,
                        line=line_num,
                        category="cert-generation",
                        severity="test",  # cert scripts are test infrastructure
                        description=f"{reason}: {stripped.strip()}",
                    )
                )

    return findings


def run_scan() -> list[Finding]:
    """Scan all container definitions for FIPS compliance issues."""
    all_findings: list[Finding] = []

    # Scan all Dockerfiles.
    for dockerfile in sorted(MZ_ROOT.rglob("Dockerfile")):
        # Skip vendored/third-party directories.
        rel = str(dockerfile.relative_to(MZ_ROOT))
        if any(skip in rel for skip in ["target/", ".git/", "venv/", "node_modules/"]):
            continue
        all_findings.extend(scan_dockerfile(dockerfile))

    # Also scan Dockerfile.* variants.
    for dockerfile in sorted(MZ_ROOT.rglob("Dockerfile.*")):
        rel = str(dockerfile.relative_to(MZ_ROOT))
        if any(skip in rel for skip in ["target/", ".git/", "venv/", "node_modules/"]):
            continue
        all_findings.extend(scan_dockerfile(dockerfile))

    # Scan certificate generation scripts.
    for script in sorted(MZ_ROOT.rglob("create-certs.sh")):
        all_findings.extend(scan_cert_scripts(script))
    for script in sorted(MZ_ROOT.rglob("gen-certs.sh")):
        all_findings.extend(scan_cert_scripts(script))

    return all_findings


def print_report(findings: list[Finding]) -> None:
    """Print a human-readable report."""
    print("=== Container FIPS Compliance Report ===")
    print()

    prod_findings = [f for f in findings if f.severity == "production"]
    test_findings = [f for f in findings if f.severity == "test"]

    if prod_findings:
        print("--- PRODUCTION images (MUST be FIPS-compliant) ---")
        print()
        # Group by file.
        files: dict[str, list[Finding]] = {}
        for f in prod_findings:
            files.setdefault(f.file, []).append(f)
        for file_path in sorted(files):
            print(f"  {file_path}:")
            for f in files[file_path]:
                loc = f":{f.line}" if f.line else ""
                print(f"    [{f.category}] {f.description} ({file_path}{loc})")
            print()

    if test_findings:
        print("--- TEST/DEV images (informational) ---")
        print()
        files = {}
        for f in test_findings:
            files.setdefault(f.file, []).append(f)
        for file_path in sorted(files):
            print(f"  {file_path}:")
            for f in files[file_path]:
                loc = f":{f.line}" if f.line else ""
                print(f"    [{f.category}] {f.description} ({file_path}{loc})")
            print()

    # Summary.
    prod_by_cat = {}
    for f in prod_findings:
        prod_by_cat[f.category] = prod_by_cat.get(f.category, 0) + 1
    test_by_cat = {}
    for f in test_findings:
        test_by_cat[f.category] = test_by_cat.get(f.category, 0) + 1

    print("=== Summary ===")
    print(f"Production findings: {len(prod_findings)}")
    for cat, count in sorted(prod_by_cat.items()):
        print(f"  {cat}: {count}")
    print(f"Test/dev findings: {len(test_findings)}")
    for cat, count in sorted(test_by_cat.items()):
        print(f"  {cat}: {count}")
    print(f"Total findings: {len(findings)}")


def print_json(findings: list[Finding]) -> None:
    """Print findings as JSON."""
    print(json.dumps([asdict(f) for f in findings], indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="lint-fips-containers",
        description="Audit container definitions for FIPS 140-3 compliance gaps.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit with code 1 if any PRODUCTION findings exist.",
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

    if args.strict:
        prod_findings = [f for f in findings if f.severity == "production"]
        if prod_findings:
            sys.exit(1)


if __name__ == "__main__":
    main()
