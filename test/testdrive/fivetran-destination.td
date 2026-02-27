# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# TODO: Reenable when database-issues#8373 is fixed
$ skip-if
SELECT true

# Exercise the Fivetran Destination.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET statement_logging_max_sample_rate = 1.0;
ALTER SYSTEM SET statement_logging_default_sample_rate = 1.0;

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
        "compression": 2,
        "encryption": 0,
        "null_string": "null-123",
        "unmodified_string": "unmodified-123"
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

# Repeating this line 1,300,000 times should get us close to 100MiB.
$ file-append container=fivetran path=c.csv compression=gzip header=a,b,c,d repeat=1300000
5000,"I am a large log line, <- can this comma mess us up?",foo_bar_baz,10

$ file-append container=fivetran path=d.csv compression=gzip header=a,b,c,d repeat=1300000
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
    "replace_files": [
        "${testdrive.fivetran-destination-files-path}/c.csv",
        "${testdrive.fivetran-destination-files-path}/d.csv"
    ],
    "update_files": [],
    "delete_files": [],
    "file_params": {
        "compression": 2,
        "encryption": 0,
        "null_string": "null-123",
        "unmodified_string": "unmodified-123"
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

# Copy a large file (previously limited by max_copy_from_size, now unlimited).

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
        "compression": 2,
        "encryption": 0,
        "null_string": "null-123",
        "unmodified_string": "unmodified-123"
    }
}
{
    "response": {
        "Success": true
    }
}

> SELECT COUNT(*) FROM foo.large;
2000000

> SELECT count(*) >= 5
  FROM mz_internal.mz_recent_activity_log
  WHERE application_name = 'materialize_fivetran_destination'
    AND sql LIKE 'COPY % FROM STDIN WITH (FORMAT CSV, HEADER false, NULL %)'
    AND finished_status = 'success';
true

> SELECT count(*)
  FROM mz_internal.mz_recent_activity_log
  WHERE application_name = 'materialize_fivetran_destination'
    AND sql LIKE 'COPY % FROM STDIN WITH (FORMAT CSV, HEADER false, NULL %)'
    AND finished_status = 'aborted';
0

# Cleanup.
> DROP SCHEMA foo CASCADE;

$ file-delete container=fivetran path=a.csv
$ file-delete container=fivetran path=b.csv
$ file-delete container=fivetran path=c.csv
$ file-delete container=fivetran path=d.csv
$ file-delete container=fivetran path=too_large.csv
