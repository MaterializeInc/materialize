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
{}

> SELECT a, b FROM foo.bar ORDER BY a DESC;
100 world
200 world
300 world
1000 hello
2000 hello
3000 hello

> CREATE TABLE foo.large (a int, b text, c text, d int)

# Repeating this line 1,400,000 times should get us close to 100MiB
$ file-append container=fivetran path=c.csv compression=gzip header=a,b,c,d repeat=1400000
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
    "replace_files": ["${testdrive.fivetran-destination-files-path}/c.csv"],
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
{}

> SELECT COUNT(*) FROM foo.large;
1400000

> SELECT * FROM foo.large LIMIT 1;
5000 "I am a large log line, <- can this comma mess us up?" foo_bar_baz 10

# Cleanup.
> DROP SCHEMA foo CASCADE;

$ file-delete container=fivetran path=a.csv
$ file-delete container=fivetran path=b.csv
$ file-delete container=fivetran path=c.csv
