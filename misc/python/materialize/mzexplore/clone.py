# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Utilities to clone DDL statements from a Materialize catalog for exploration
and experimentation.
"""

import string
import textwrap
from contextlib import closing
from itertools import chain
from typing import TextIO

from psycopg.errors import DatabaseError

from materialize.mzexplore import sql
from materialize.mzexplore.common import ClonedItem, ItemType, info, warn


def defs(
    ddl_out: TextIO,
    cmp_out: TextIO,
    database: str,
    schema: str,
    cluster: str,
    object_ids: list[str],
    db_port: int,
    db_host: str,
    db_user: str,
    db_pass: str | None,
    db_require_ssl: bool,
    mzfmt: bool,
) -> None:
    """
    Generate a DDL script that clones object_ids and dependencies that live in
    the same cluster into a different cluster.
    """

    database_new = sql.identifier(database, True)
    schema_new = sql.identifier(schema, True)
    cluster_new = sql.identifier(cluster, True)

    with closing(
        sql.Database(
            port=db_port,
            host=db_host,
            user=db_user,
            database=None,
            password=db_pass,
            require_ssl=db_require_ssl,
        )
    ) as db:
        output_template = string.Template(
            textwrap.dedent(
                """
                -- original id: $id
                $create_sql
                """
            ).lstrip()
        )

        # Extract dependencies
        # --------------------

        if len(object_ids) == 0:
            msg = "At least one object_id necessary"
            raise ValueError(msg)

        old_clusters = list(db.object_clusters(object_ids))
        if len(old_clusters) != 1:
            msg = f"Cannot find unique old cluster for object ids: {object_ids}"
            raise ValueError(msg)

        [cluster_old] = old_clusters

        if cluster_old["name"] == cluster:
            msg = f"Old and new clusters can't have the same name: {cluster}"
            raise ValueError(msg)

        # Replacement pattern for clusters.
        cluster_str_old = f"IN CLUSTER {sql.identifier(cluster_old['name'], True)}"
        cluster_str_new = f"IN CLUSTER {cluster_new}"

        items_seen = set()
        aliased_refs, index_on_refs, simple_refs = [], [], []
        for item in db.clone_dependencies(object_ids, cluster_old["id"]):
            item_database = sql.identifier(item["database"], True)
            item_schema = sql.identifier(item["schema"], True)
            item_name = sql.identifier(item["name"], True)
            fqname = f"{item_database}.{item_schema}.{item_name}"

            try:
                item_type = ItemType(item["type"])
            except ValueError:
                warn(f"Unsupported item type `{item['type']}` for {fqname}")
                continue

            cloned_item = ClonedItem(
                database=item["database"],
                schema=item["schema"],
                name=item["name"],
                id=item["id"],
                item_type=item_type,
            )

            show_create_query = item_type.show_create(fqname)
            if show_create_query is None:
                msg = f"Cannot determine SHOW CREATE query for {fqname}"
                raise ValueError(msg)

            assert cloned_item not in items_seen
            items_seen.add(cloned_item)

            # Add replacement string pairs
            (create_name_old, create_name_new) = (
                cloned_item.create_name_old(),
                cloned_item.create_name_new(database_new, schema_new),
            )
            aliased_refs.append(
                (
                    cloned_item.aliased_ref_old(),
                    cloned_item.aliased_ref_new(database_new, schema_new),
                )
            )
            index_on_refs.append(
                (
                    cloned_item.index_on_ref_old(),
                    cloned_item.index_on_ref_new(database_new, schema_new),
                )
            )
            simple_refs.append(
                (
                    cloned_item.simple_ref_old(),
                    cloned_item.simple_ref_new(database_new, schema_new),
                )
            )

            try:
                info(f"Appending DDL for {item_type.sql()} {fqname}")
                sql_old = db.query_one(show_create_query)["create_sql"]

                sql_new = sql_old
                sql_new = sql_new.replace(create_name_old, create_name_new, 1)
                sql_new = sql_new.replace(cluster_str_old, cluster_str_new, 1)
                # Aliases need to be replaced in this order (simple_refs last).
                for ref_old, ref_new in chain(aliased_refs, index_on_refs, simple_refs):
                    sql_new = sql_new.replace(ref_old, ref_new)

                sql_old = sql.try_mzfmt(sql_old) if mzfmt else sql_old
                sql_old = sql_old.rstrip() + "\n"
                sql_new = sql.try_mzfmt(sql_new) if mzfmt else sql_new
                sql_new = sql_new.rstrip() + "\n"

                # Write the original definition into cmp_out.
                cmp_out.write(
                    output_template.substitute(dict(create_sql=sql_old, **item))
                )
                # Write the modified definition into ddl_out.
                ddl_out.write(
                    output_template.substitute(dict(create_sql=sql_new, **item))
                )
            except DatabaseError as e:
                warn(f"Cannot append DDL for {item_type.sql()} {fqname}: {e}")
