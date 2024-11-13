# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import logging
from typing import Any

import pg8000
import requests
import sqlparse
from kubernetes.client import V1Service
from pg8000 import Connection, Cursor

from materialize.cloudtest.k8s.api.k8s_resource import K8sResource

LOGGER = logging.getLogger(__name__)


class K8sService(K8sResource):
    service: V1Service

    def kind(self) -> str:
        return "service"

    def create(self) -> None:
        core_v1_api = self.api()
        core_v1_api.create_namespaced_service(
            body=self.service, namespace=self.namespace()
        )

    def node_port(self, name: str | None = None) -> int:
        assert self.service and self.service.metadata and self.service.metadata.name
        service = self.api().read_namespaced_service(
            self.service.metadata.name, self.namespace()
        )
        assert service is not None

        spec = service.spec
        assert spec is not None

        ports = spec.ports
        assert ports is not None and len(ports) > 0

        port = next(p for p in ports if name is None or p.name == name)

        node_port = port.node_port
        assert node_port is not None

        return node_port

    def sql_conn(
        self,
        port: str | None = None,
        user: str = "materialize",
    ) -> Connection:
        """Get a connection to run SQL queries against the service"""
        return pg8000.connect(
            host="localhost",
            port=self.node_port(name=port),
            user=user,
        )

    def sql_cursor(
        self,
        port: str | None = None,
        user: str = "materialize",
        autocommit: bool = True,
    ) -> Cursor:
        """Get a cursor to run SQL queries against the service"""
        conn = self.sql_conn(port=port, user=user)
        conn.autocommit = autocommit
        return conn.cursor()

    def sql(
        self,
        sql: str,
        port: str | None = None,
        user: str = "materialize",
    ) -> None:
        """Run a batch of SQL statements against the service."""
        with self.sql_cursor(port=port, user=user) as cursor:
            for statement in sqlparse.split(sql):
                LOGGER.info(f"> {statement}")
                cursor.execute(statement)

    def sql_query(
        self,
        sql: str,
        port: str | None = None,
        user: str = "materialize",
    ) -> Any:
        """Execute a SQL query against the service and return results."""
        with self.sql_cursor(port=port, user=user) as cursor:
            LOGGER.info(f"> {sql}")
            cursor.execute(sql)
            return cursor.fetchall()

    def http_get(self, path: str) -> Any:
        url = f"http://localhost:{self.node_port('internalhttp')}/{path}"
        response = requests.get(url)
        response.raise_for_status()
        return response.text
