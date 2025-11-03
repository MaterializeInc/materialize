#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Helper script to resolve version aliases to actual version tags.

Usage:
    ./resolve_version.py cloud-backward  # prints: v0.99.0
    ./resolve_version.py sm-lts          # prints: v25.1.3
    
    # Use in shell:
    export MZ_VERSION=$(./resolve_version.py cloud-backward)
"""

import sys
from pathlib import Path

# Add parent directories to path to import materialize modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from materialize.console_version_matrix import get_console_test_versions
from materialize.mz_version import MzVersion


def main():
    if len(sys.argv) != 2:
        print("Usage: resolve_version.py <version-alias>", file=sys.stderr)
        print("\nAvailable aliases:", file=sys.stderr)
        print("  cloud-backward  - Last released minor version", file=sys.stderr)
        print("  cloud-current   - Current development version (uses source)", file=sys.stderr)
        print("  cloud-forward   - Forward compatible version (uses source)", file=sys.stderr)
        print("  sm-lts          - Self-Managed LTS", file=sys.stderr)
        print("\nOr provide a direct version like: v0.100.0", file=sys.stderr)
        sys.exit(1)
    
    version_alias = sys.argv[1]
    
    # Check if it's a direct version string
    if version_alias.startswith("v"):
        print(version_alias)
        return
    
    # Otherwise, resolve from version matrix
    versions = get_console_test_versions()
    
    if version_alias not in versions:
        print(f"Error: Unknown version alias: {version_alias}", file=sys.stderr)
        print(f"Available aliases: {', '.join(versions.keys())}", file=sys.stderr)
        sys.exit(1)
    
    version = versions[version_alias]
    if version:
        print(str(version))
    else:
        # Return empty for "use source" versions
        print("")


if __name__ == "__main__":
    main()

