## Unified materialized binary

The binary `materialized` is a unified binary that provides access to both `environmentd` and `clusterd`, and it
selects between the two based on the path that invokes the binary. It expects to the symlinked (or copied) to
`environmentd` and `clusterd` to switch its behavior.

The sole purpose of this binary is to reduce the size of the docker `materialized` docker container, which we use
in tests. It is not intended for production use.
