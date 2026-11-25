#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Fail if any lockfile pins a package that OSV lists as malicious (MAL-*).

Known vulnerabilities are left to cargo-deny and Renovate. osv-scanner runs
with no host mounts or Docker socket. The lockfiles are copied in, so a
compromised scanner can only lie about its verdict.
"""

import io
import json
import subprocess
import sys
import tarfile

# renovate: datasource=docker depName=ghcr.io/google/osv-scanner
OSV_SCANNER_IMAGE = "ghcr.io/google/osv-scanner:v2.7.1@sha256:a70ffffc2bfd33482953e5d6b64c6fcdb30239717a07b0f0d3ab2859ce3e4cde"

LOCKFILES = [
    "Cargo.lock",
    "package-lock.json",
    "yarn.lock",
    "requirements*.txt",
    "uv.lock",
    "pom.xml",
    "Gemfile.lock",
]


def lockfiles() -> list[str]:
    patterns = [f":(glob)**/{name}" for name in LOCKFILES]
    out = subprocess.check_output(["git", "ls-files", "-z", "--", *patterns])
    return [p for p in out.decode().split("\0") if p]


def scan(paths: list[str]) -> dict:
    # Transitive resolution of manifests (pom.xml, requirements.txt) queries
    # package registries but runs no package code. Call analysis would run
    # Cargo build scripts, so it stays off.
    container = subprocess.check_output(
        [
            "docker",
            "create",
            "--user=65534:65534",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--pids-limit=256",
            OSV_SCANNER_IMAGE,
            "scan",
            "source",
            "--format=json",
            *(f"--lockfile=/src/{p}" for p in paths),
        ],
        text=True,
    ).strip()
    try:
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            for p in paths:
                tar.add(p, arcname=f"src/{p}")
        subprocess.run(
            ["docker", "cp", "-", f"{container}:/"], input=buf.getvalue(), check=True
        )
        result = subprocess.run(
            ["docker", "start", "--attach", container], stdout=subprocess.PIPE
        )
    finally:
        subprocess.run(["docker", "rm", container], stdout=subprocess.DEVNULL)
    # osv-scanner exits 1 when it finds any vulnerability, malicious or not.
    if result.returncode not in (0, 1):
        sys.exit(f"osv-scanner failed with exit {result.returncode}")
    return json.loads(result.stdout)


def main() -> int:
    paths = lockfiles()
    findings = []
    for res in scan(paths).get("results") or []:
        source = res["source"]["path"].removeprefix("/src/")
        for pkg in res.get("packages") or []:
            for vuln in pkg.get("vulnerabilities") or []:
                ids = [vuln["id"], *(vuln.get("aliases") or [])]
                if any(i.startswith("MAL-") for i in ids):
                    p = pkg["package"]
                    findings.append(
                        f"{source}: {p['ecosystem']} {p['name']} {p['version']}"
                        f" ({', '.join(ids)})"
                    )

    print(f"Checked {len(paths)} lockfiles against OSV malicious-package advisories.")
    if findings:
        print("Packages with malicious-package advisories:")
        for f in sorted(findings):
            print(f"  {f}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
