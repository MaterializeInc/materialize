# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Exercise the Fivetran Destination.

> SELECT 1;
1

> CREATE SCHEMA IF NOT EXISTS foo;

> CREATE TABLE foo.bar (a int, b text);

# To identify primary keys we leave a magic comment.
> COMMENT ON COLUMN foo.bar.a IS 'mz_is_primary_key';

$ fivetran-destination action=describe
{
    "schema_name": "foo",
    "table_name": "bar"
}
{
    "response": {
        "Table": {
            "name": "bar",
            "columns": [
                {
                    "name": "a",
                    "type": 3,
                    "primary_key": true
                },
                {
                    "name": "b",
                    "type": 13,
                    "primary_key": false
                }
            ]
        }
    }
}

$ file-append container=fivetran path=a.csv compression=gzip
a,b
1000,hello
2000,hello
3000,hello

# Note: The columns on the table are in the opposite order, the Fivetran Destination should re-map
# them.
$ file-append container=fivetran path=b.csv compression=gzip
b,a
world,100
world,200
world,300

$ fivetran-destination action=write_batch
{
    "schema_name": "foo",
    "table_name": "bar",
    "table": {
        "name": "bar",
        "columns": [
            {
                "name": "a",
                "type": 3,
                "primary_key": true
            },
            {
                "name": "b",
                "type": 13,
                "primary_key": false
            }
        ]
    },
    "keys": {},
    "replace_files": [
        "${testdrive.fivetran-destination-files-path}/a.csv",
        "${testdrive.fivetran-destination-files-path}/b.csv"
    ],
    "update_files": [],
    "delete_files": [],
    "file_params": {
        "Csv": {
            "compression": 2,
            "encryption": 0,
            "null_string": "null-123",
            "unmodified_string": "unmodified-123"
        }
    }
}
{
    "response": {
        "Success": true
    }
}

> SELECT a, b FROM foo.bar ORDER BY a DESC;
100 world
200 world
300 world
1000 hello
2000 hello
3000 hello

> CREATE TABLE foo.large (a int, b text, c text, d int)

# Set the max copy from size to 100MiB.
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET max_copy_from_size = 104857600;

# Repeating this line 1,300,000 times should get us close to 100MiB.
$ file-append container=fivetran path=c.csv compression=gzip header=a,b,c,d repeat=1300000
5000,"I am a large log line, <- can this comma mess us up?",foo_bar_baz,10

$ file-append container=fivetran path=d.csv compression=gzip header=a,b,c,d repeat=1300000
5000,"I am a large log line, <- can this comma mess us up?",foo_bar_baz,10

# Note: Both 'c.csv' and 'd.csv' are individually under the 'max_copy_from_size', but together
# they exceed it. We want to make sure the write_batch still succeeds in this case.

# TODO(def-) Reenable when database-issues#8373 is fixed
$ skip-if
SELECT true

$ fivetran-destination action=write_batch
{
    "schema_name": "foo",
    "table_name": "large",
    "table": {
        "name": "large",
        "columns": [
            {
                "name": "a",
                "type": 3,
                "primary_key": true
            },
            {
                "name": "b",
                "type": 13,
                "primary_key": false
            },
            {
                "name": "c",
                "type": 13,
                "primary_key": false
            },
            {
                "name": "d",
                "type": 3,
                "primary_key": false
            }
        ]
    },
    "keys": {},
    "replace_files": [
        "${testdrive.fivetran-destination-files-path}/c.csv",
        "${testdrive.fivetran-destination-files-path}/d.csv"
    ],
    "update_files": [],
    "delete_files": [],
    "file_params": {
        "Csv": {
            "compression": 2,
            "encryption": 0,
            "null_string": "null-123",
            "unmodified_string": "unmodified-123"
        }
    }
}
{
    "response": {
        "Success": true
    }
}

> SELECT COUNT(*) FROM foo.large;
2600000

> SELECT * FROM foo.large LIMIT 1;
5000 "I am a large log line, <- can this comma mess us up?" foo_bar_baz 10

# Try copying a file that is over the limit.

$ file-append container=fivetran path=too_large.csv compression=gzip header=a,b,c,d repeat=2000000
5000,"I am a large log line, <- can this comma mess us up?",foo_bar_baz,10

$ fivetran-destination action=write_batch
{
    "schema_name": "foo",
    "table_name": "large",
    "table": {
        "name": "large",
        "columns": [
            {
                "name": "a",
                "type": 3,
                "primary_key": true
            },
            {
                "name": "b",
                "type": 13,
                "primary_key": false
            },
            {
                "name": "c",
                "type": 13,
                "primary_key": false
            },
            {
                "name": "d",
                "type": 3,
                "primary_key": false
            }
        ]
    },
    "keys": {},
    "replace_files": ["${testdrive.fivetran-destination-files-path}/too_large.csv"],
    "update_files": [],
    "delete_files": [],
    "file_params": {
        "Csv": {
            "compression": 2,
            "encryption": 0,
            "null_string": "null-123",
            "unmodified_string": "unmodified-123"
        }
    }
}
{
    "response": {
        "Failure": "error when calling Materialize: Error { kind: Db, cause: Some(DbError { severity: \"ERROR\", parsed_severity: None, code: SqlState(E53000), message: \"COPY FROM STDIN too large\", detail: None, hint: None, position: None, where_: None, schema: None, table: None, column: None, datatype: None, constraint: None, file: None, line: None, routine: None }) }: closing sink: replace_files: replace files: write_batch"
    }
}

# No new data should have been inserted.
> SELECT COUNT(*) FROM foo.large;
2600000

# Cleanup.
> DROP SCHEMA foo CASCADE;

$ file-delete container=fivetran path=a.csv
$ file-delete container=fivetran path=b.csv
$ file-delete container=fivetran path=c.csv
$ file-delete container=fivetran path=d.csv
$ file-delete container=fivetran path=too_large.csv
