# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import pandas as pd


class DfWrapperBase:
    """Wrapper for data frames."""

    def __init__(self, data: pd.DataFrame):
        self.data = data

    def length(self) -> int:
        return len(self.data.index)

    def has_values(self) -> bool:
        return self.length() > 0

    def to_csv(self, file_path: Path) -> None:
        self.data.to_csv(file_path)


def concat_df_wrapper_data(wrappers: Iterable[DfWrapperBase]) -> pd.DataFrame:
    return pd.concat(
        [wrapper.data for wrapper in wrappers],
        ignore_index=True,
    )
