# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


# Features Materialize that require non-standard infrastructure to test.
#
# For example, the SQL Server Docker image is only supported on x86_64 so we
# cannot enable that feature on ARM based runners.
class Features:
    AZURITE = "azurite"

    def __init__(self, features):
        self.features = features

    def azurite_enabled(self) -> bool:
        return self.features and self.AZURITE in self.features
