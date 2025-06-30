# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose.service import Service, ServiceConfig


class SqlServer(Service):
    DEFAULT_USER = "SA"
    DEFAULT_SA_PASSWORD = "RPSsql12345"

    def __init__(
        self,
        # The password must be at least 8 characters including uppercase,
        # lowercase letters, base-10 digits and/or non-alphanumeric symbols.
        sa_password: str = DEFAULT_SA_PASSWORD,
        name: str = "sql-server",
        mzbuild: str = "sql-server",
        image: str | None = None,
        platform: str | None = None,
        environment_extra: list[str] = [],
    ) -> None:

        config: ServiceConfig = {"image": image} if image else {"mzbuild": mzbuild}

        # If using the official MSSQL docker image, the requested image's platform (linux/amd64) won't match the detected host platform on mac (linux/arm64/v8).
        # in that case, the platform "linux/amd64" must be specified.
        # See https://github.com/microsoft/mssql-docker/issues/802 for current status.
        if platform:
            config["platform"] = platform

        config.update(
            {
                "ports": [1433],
                "environment": [
                    f"SA_PASSWORD={sa_password}",
                    *environment_extra,
                ],
            }
        )
        super().__init__(
            name=name,
            config=config,
        )
        self.sa_password = sa_password
