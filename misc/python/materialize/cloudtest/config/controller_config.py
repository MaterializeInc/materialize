# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from dataclasses import dataclass


@dataclass
class ControllerDefinition:
    name: str
    default_port: str
    is_configurable: bool


CONTROLLER_DEFINITIONS = [
    ControllerDefinition("region-controller", "8002", False),
    ControllerDefinition("environment-controller", "8001", False),
    ControllerDefinition("sync-server", "8003", True),
    ControllerDefinition("internal-api-server", "8032", True),
    ControllerDefinition("region-api-server", "8033", True),
]

CONTROLLER_NAMES = [controller.name for controller in CONTROLLER_DEFINITIONS]
