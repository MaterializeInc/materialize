# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from typing import Literal, TypedDict

from materialize.mzcompose.service import (
    Service,
    ServiceConfig,
    ServiceDependency,
)


class DnsmasqEntry(TypedDict):
    type: Literal["cname", "address"]
    key: str
    value: str


def dnsmask_entry_to_rec(e: DnsmasqEntry) -> str:
    match e["type"]:
        case "address":
            return f"--entry=address=/{e['key']}/{e['value']}"
        case "cname":
            return f"--entry=cname={e['key']},{e['value']}"


class Dnsmasq(Service):

    def __init__(
        self,
        name: str = "dnsmasq",
        mzbuild: str = "dnsmasq",
        command: list[str] | None = None,
        depends_on: list[str] = [],
        networks: (
            dict[str, dict[str, list[str]]]
            | dict[str, dict[str, str]]
            | list[str]
            | None
        ) = None,
        dns_overrides: list[DnsmasqEntry] = [],
    ) -> None:

        command = [dnsmask_entry_to_rec(e) for e in dns_overrides]

        depends_graph: dict[str, ServiceDependency] = {
            s: {"condition": "service_started"} for s in depends_on
        }
        config: ServiceConfig = {
            "mzbuild": mzbuild,
            "command": command,
            "ports": [
                "53:53/tcp",
                "53:53/udp",
            ],
            "depends_on": depends_graph,
            "allow_host_ports": True,
        }
        if networks:
            config["networks"] = networks

        super().__init__(
            name=name,
            config=config,
        )
