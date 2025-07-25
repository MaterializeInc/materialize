# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose.service import Service, ServiceHealthcheck


def azure_blob_uri(address: str = "azurite") -> str:
    return f"http://devstoreaccount1.{address}:10000/container"


class Azurite(Service):
    def __init__(
        self,
        name: str = "azurite",
        aliases: list[str] = ["azurite", "devstoreaccount1.azurite"],
        command: list[str] | None = None,
        in_memory: bool = False,
        healthcheck: ServiceHealthcheck | None = None,
        stop_grace_period: str = "120s",
        ports: list[int | str] = [10000],
        allow_host_ports: bool = False,
    ):
        if command is None:
            command = [
                "azurite-blob",
                "--blobHost",
                "0.0.0.0",
                "--blobPort",
                "10000",
                "--disableProductStyleUrl",
                "--loose",
            ]

        if in_memory:
            command.append("--inMemoryPersistence")

        if healthcheck is None:
            healthcheck = {
                "test": "nc 127.0.0.1 10000 -z",
                "interval": "1s",
                "start_period": "30s",
            }

        super().__init__(
            name=name,
            config={
                "mzbuild": "azurite",
                "networks": {"default": {"aliases": aliases}},
                "ports": ports,
                "allow_host_ports": allow_host_ports,
                "command": command,
                "init": True,
                "healthcheck": healthcheck,
                "stop_grace_period": stop_grace_period,
            },
        )
