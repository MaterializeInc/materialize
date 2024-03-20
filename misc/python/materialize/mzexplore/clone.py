# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Utilities to extract data from a Materialize catalog for exploration purposes.
"""

import string
import textwrap
from contextlib import closing
from typing import TextIO

from pg8000.dbapi import DatabaseError

from materialize.mzexplore import sql
from materialize.mzexplore.common import ItemType, info, warn


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

    The catalog items included in the script will be seeded from the .
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

        items_sub = dict()
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

            show_create_query = item_type.show_create(fqname)
            if show_create_query is None:
                continue

            if item_type == ItemType.INDEX:
                assert item_name not in items_sub
                name_new = sql.identifier(
                    "_".join(
                        [
                            item["name"],
                            item["id"],
                        ]
                    ),
                    True,
                )
                items_sub[item_name] = name_new
            else:
                assert fqname not in items_sub
                name_new = sql.identifier(
                    "_".join(
                        [
                            item["database"],
                            item["schema"],
                            item["name"],
                            item["id"],
                        ]
                    ),
                    True,
                )
                items_sub[fqname] = f"{database_new}.{schema_new}.{name_new}"

            cluster_str_old = f"IN CLUSTER {sql.identifier(cluster_old['name'], True)}"
            cluster_str_new = f"IN CLUSTER {cluster_new}"
            try:
                info(f"Appending DDL for {item_type.sql()} {fqname}")
                sql_old = db.query_one(show_create_query)["create_sql"]

                sql_new = sql_old.replace(cluster_str_old, cluster_str_new)
                for old_item_str, new_item_str in items_sub.items():
                    sql_new = sql_new.replace(old_item_str, new_item_str)

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
                warn(f"Cannot export def {fqname}: {e}")
