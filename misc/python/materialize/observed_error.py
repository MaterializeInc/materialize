# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from dataclasses import dataclass, field


@dataclass(unsafe_hash=True)
class ObservedBaseError:
    internal_error_type: str
    occurrences: int = field(init=False, compare=False, hash=False)

    def to_text(self) -> str:
        raise NotImplementedError

    def to_markdown(self) -> str:
        raise NotImplementedError

    def occurrences_to_markdown(self) -> str:
        if self.occurrences < 2:
            return ""

        return f"\n({self.occurrences} occurrences)"


@dataclass(unsafe_hash=True)
class WithIssue:
    issue_number: int
    issue_url: str
    issue_title: str
