# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import threading

import pg8000


def execute(cur: pg8000.Cursor, query: str) -> bool:
    query += ";"
    name = threading.current_thread().getName()
    try:
        try:
            cur.execute(query)
        except Exception as e:
            # TODO: Proper error handling
            if "in failed transaction block" in str(e):
                cur.connection.rollback()
                cur.execute("SET TRANSACTION_ISOLATION TO SERIALIZABLE")
                cur.execute(query)
            elif "current transaction is aborted" in str(e):
                cur.connection.rollback()
                cur.execute("SET TRANSACTION_ISOLATION TO SERIALIZABLE")
                cur.execute(query)
            elif "in the same timedomain" in str(e):
                cur.connection.commit()
                cur.execute("SET TRANSACTION_ISOLATION TO SERIALIZABLE")
                cur.execute(query)
            elif "transaction in write-only mode" in str(e):
                cur.connection.commit()
                cur.execute("SET TRANSACTION_ISOLATION TO SERIALIZABLE")
                cur.execute(query)
            elif "transaction in read-only mode" in str(e):
                cur.connection.commit()
                cur.execute("SET TRANSACTION_ISOLATION TO SERIALIZABLE")
                cur.execute(query)
            elif "writes to a single table" in str(e):
                cur.connection.commit()
                cur.execute("SET TRANSACTION_ISOLATION TO SERIALIZABLE")
                cur.execute(query)
            else:
                raise e
    except Exception as e:
        if "in failed transaction block" in str(e):
            cur.connection.rollback()
            cur.execute("SET TRANSACTION_ISOLATION TO SERIALIZABLE")
            return False
        if "current transaction is aborted" in str(e):
            cur.connection.rollback()
            cur.execute("SET TRANSACTION_ISOLATION TO SERIALIZABLE")
            return False
        if "in the same timedomain" in str(e):
            cur.connection.commit()
            cur.execute("SET TRANSACTION_ISOLATION TO SERIALIZABLE")
            return False
        if "transaction in write-only mode" in str(e):
            cur.connection.commit()
            cur.execute("SET TRANSACTION_ISOLATION TO SERIALIZABLE")
            return False
        if "transaction in read-only mode" in str(e):
            cur.connection.commit()
            cur.execute("SET TRANSACTION_ISOLATION TO SERIALIZABLE")
            return False
        if "writes to a single table" in str(e):
            cur.connection.commit()
            cur.execute("SET TRANSACTION_ISOLATION TO SERIALIZABLE")
            return False
        if "unknown catalog item" in str(e):
            return False
        if "already exists" in str(e):
            return False
        if "does not exist" in str(e):
            return False
        if "query could not complete" in str(e):
            return False
        if "cached plan must not change result type" in str(e):
            return False
        if "violates not-null constraint" in str(e):
            return False
        print(f"{name}: Query failed: {query}\nError: {e}")
        raise e

    return True
