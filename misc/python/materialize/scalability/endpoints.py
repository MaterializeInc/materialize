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
        super().__init__()

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

    def name(self) -> str:
        return "postgres"


class MaterializeNonRemote(Endpoint):
    def host(self) -> str:
        return "localhost"

    def user(self) -> str:
        return "materialize"

    def password(self) -> str:
        return "materialize"

    def priv_port(self) -> int:
        raise NotImplementedError

    def lift_limits(self) -> None:
        priv_conn = pg8000.connect(
            host=self.host(), user="mz_system", port=self.priv_port()
        )
        priv_conn.autocommit = True
        priv_cursor = priv_conn.cursor()
        priv_cursor.execute("ALTER SYSTEM SET max_connections = 65535;")
        priv_cursor.execute("ALTER SYSTEM SET max_tables = 65535;")


class MaterializeLocal(MaterializeNonRemote):
    """Connect to a Materialize instance running on the local host"""

    def port(self) -> int:
        return 6875

    def priv_port(self) -> int:
        return 6877

    def up(self) -> None:
        self.lift_limits()


class MaterializeContainer(MaterializeNonRemote):
    def __init__(self, composition: Composition, image: Optional[str] = None) -> None:
        self.composition = composition
        self.image = image
        self._port: Optional[int] = None
        super().__init__()

    def port(self) -> int:
        assert self._port is not None
        return self._port

    def priv_port(self) -> int:
        return self.composition.port("materialized", 6877)

    def up(self) -> None:
        self.composition.down(destroy_volumes=True)
        with self.composition.override(Materialized(image=self.image)):
            self.composition.up("materialized")
            self._port = self.composition.default_port("materialized")

        self.lift_limits()
