# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Utilities to analyze data extracted from a Materialize catalog.
"""

from pathlib import Path
from textwrap import dedent
from typing import TextIO

from materialize.mzexplore import sql
from materialize.mzexplore.common import explain_diff, explain_file, info, warn


def changes(
    out: TextIO,
    target: Path,
    header_name: str,
    base_suffix: str,
    diff_suffix: str,
) -> None:
    """
    Append sections to an `*.md` file with items corresponding to changes pairs
    of optimized plans for the same catalog item extracted with the given `base`
    and `diff` suffix.
    """

    # Ensure that the target dir exists
    if not target.is_dir():
        warn(f"Target path `{target}` is not a folder")
        return

    info(f"Comparing `{base_suffix}` vs `{diff_suffix}` " f"plans in {target}")

    info("Comparing optimized plans")
    out.write(
        dedent(
            f"""
            ## {header_name}

            """
        ).lstrip("\n")
    )

    for base_path in target.glob(f"**/*.optimized_plan.{base_suffix}.txt"):
        base = explain_file(base_path)
        if base is None:
            warn(f"File {base_path} is not recognized as an ExplainFile")
            continue

        diff = explain_diff(base=base, diff_suffix=diff_suffix)
        if not (target / diff.path()).is_file():
            warn(f"Cannot find diff file {diff.file_name()} for {base}")
            continue

        item_type = base.item_type
        database = sql.identifier(base.database)
        schema = sql.identifier(base.schema)
        name = sql.identifier(base.name)

        base_data = (target / base.path()).read_text(encoding="utf8")
        diff_data = (target / diff.path()).read_text(encoding="utf8")

        if base_data != diff_data:
            info(f"Found diff at {item_type.sql()} `{database}.{schema}.{name}`")

            out.write(
                dedent(
                    f"""
                    - TODO(REGRESSION|IMPROVEMENT) in {item_type.sql()} `{database}.{schema}.{name}`

                      ```bash
                      code --diff \\
                          {target / base.path()} \\
                          {target / diff.path()}
                      ```

                    """
                ).lstrip("\n")
            )
