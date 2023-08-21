# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Optional

import pg8000

from materialize.mzcompose import Composition
from materialize.mzcompose.services import Materialized, Postgres
from materialize.scalability.endpoint import Endpoint


class MaterializeLocal(Endpoint):
    """Connect to a Materialize instance running on the local host"""

    def host(self) -> str:
        return "localhost"

    def port(self) -> int:
        return 6875

    def user(self) -> str:
        return "materialize"

    def password(self) -> str:
        return "materialize"

    def up(self) -> None:
        priv_conn = pg8000.connect(
            host=self.host(), user="mz_system", port=self.port() + 2
        )
        priv_conn.autocommit = True
        cursor = priv_conn.cursor()
        cursor.execute("ALTER SYSTEM SET max_connections = 65535")


class MaterializeRemote(Endpoint):
    """Connect to a remote Materialize instance using a psql URL"""

    def __init__(self, materialize_url: str) -> None:
        self.materialize_url = materialize_url

    def url(self) -> str:
        return self.materialize_url

    def up(self) -> None:
        pass


class PostgresContainer(Endpoint):
    def __init__(self, composition: Composition) -> None:
        self.composition = composition
        self._port: Optional[int] = None

    def host(self) -> str:
        return "localhost"

    def port(self) -> int:
        assert self._port is not None
        return self._port

    def user(self) -> str:
        return "postgres"

    def password(self) -> str:
        return "postgres"

    def up(self) -> None:
        self.composition.down(destroy_volumes=True)
        with self.composition.override(Postgres()):
            self.composition.up("postgres")
            self._port = self.composition.default_port("postgres")


class MaterializeContainer(Endpoint):
    def __init__(self, composition: Composition, image: Optional[str] = None) -> None:
        self.composition = composition
        self.image = image
        self._port: Optional[int] = None

    def host(self) -> str:
        return "localhost"

    def port(self) -> int:
        assert self._port is not None
        return self._port

    def user(self) -> str:
        return "materialize"

    def password(self) -> str:
        return "materialize"

    def up(self) -> None:
        self.composition.down(destroy_volumes=True)
        with self.composition.override(Materialized(image=self.image)):
            self.composition.up("materialized")
            self._port = self.composition.default_port("materialized")
