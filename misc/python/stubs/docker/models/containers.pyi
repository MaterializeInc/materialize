# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Any, Dict, List

class Container:
    attrs: Dict[str, Any]
    def reload(self) -> None: ...

class ContainerCollection:
    def run(
        self,
        image: str,
        args: List[str] = ...,
        network_mode: str = ...,
        detach: bool = ...,
    ) -> Container: ...
