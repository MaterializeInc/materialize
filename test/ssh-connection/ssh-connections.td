# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# Test SSH key generation and management for SSH connections

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_connection_validation_syntax = true

> SELECT * FROM mz_ssh_tunnel_connections;
id public_key_1 public_key_2
----------------------------

> CREATE CONNECTION louisoix TO SSH TUNNEL (
    HOST 'chaos.example.com',
    USER 'louisoix',
    PORT 22
  );

# mz_ssh_tunnel_connections is properly populated, and SSH public keys look like keys
> SELECT name, public_key_1 LIKE 'ssh-ed25519%' pkey1, public_key_2 LIKE 'ssh-ed25519%' pkey2
  FROM mz_ssh_tunnel_connections
  JOIN mz_connections USING (id);
name     pkey1 pkey2
--------------------
louisoix true  true

> CREATE CONNECTION omega TO SSH TUNNEL (
    HOST 'chaos.example.com',
    USER 'omega',
    PORT 22
  );

> SELECT name, public_key_1 LIKE 'ssh-ed25519%' pkey1, public_key_2 LIKE 'ssh-ed25519%' pkey2
  FROM mz_ssh_tunnel_connections
  JOIN mz_connections USING (id);
name     pkey1 pkey2
--------------------
louisoix true  true
omega    true  true

> DROP CONNECTION louisoix;

# SSH connections can be normally dropped
> SELECT name, public_key_1 LIKE 'ssh-ed25519%' pkey1, public_key_2 LIKE 'ssh-ed25519%' pkey2
  FROM mz_ssh_tunnel_connections
  JOIN mz_connections USING (id);
name     pkey1 pkey2
--------------------
omega    true  true

# Key rotation doesn't fail
> ALTER CONNECTION omega ROTATE KEYS;

> SELECT name, public_key_1 LIKE 'ssh-ed25519%' pkey1, public_key_2 LIKE 'ssh-ed25519%' pkey2
  FROM mz_ssh_tunnel_connections
  JOIN mz_connections USING (id);
name     pkey1 pkey2
--------------------
omega    true  true

> DROP CONNECTION omega;

# Connections can still be dropped after rotating keys
> SELECT name, public_key_1 LIKE 'ssh-ed25519%' pkey1, public_key_2 LIKE 'ssh-ed25519%' pkey2
  FROM mz_ssh_tunnel_connections
  JOIN mz_connections USING (id);
name     pkey1 pkey2
--------------------

> CREATE CONNECTION phoenix TO SSH TUNNEL (
    HOST 'light.example.com',
    USER 'phoenix',
    PORT 22
  );

> CREATE CONNECTION yshtola TO POSTGRES (
    HOST 'linsa.example.com',
    PORT 5432,
    DATABASE 'source',
    USER 'yshtola',
    SSH TUNNEL phoenix
  ) WITH (VALIDATE = false);

! CREATE CONNECTION papalymo TO POSTGRES (
    HOST 'gridania.example.com',
    PORT 5432,
    DATABASE 'source',
    USER 'yshtola',
    SSH TUNNEL johto
  );
contains: unknown catalog item 'johto'

! CREATE CONNECTION papalymo TO POSTGRES (
    HOST 'gridania.example.com',
    PORT 5432,
    DATABASE 'source',
    USER 'yshtola',
    SSH TUNNEL yshtola
  );
contains: yshtola is not an SSH connection

> SELECT name, type FROM mz_connections;
name    type
----------------
phoenix ssh-tunnel
yshtola postgres

# SSH tunnel dependencies are properly tracked
! DROP CONNECTION phoenix;
contains: still depended upon

> DROP CONNECTION phoenix CASCADE;

> SELECT name, type FROM mz_connections;
name    type
----------------
