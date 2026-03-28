# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose.service import Service


class DynamodbLocal(Service):
    def __init__(
        self,
        name: str = "dynamodb",
    ):

        super().__init__(
            name=name,
            config={
                "image": "amazon/dynamodb-local:3.3.0",
                "command": [
                    "-Djava.library.path=./DynamoDBLocal_lib",
                    "-jar",
                    "DynamoDBLocal.jar",
                    "-disableTelemetry",
                    "-delayTransientStatuses",
                ],
            },
        )
