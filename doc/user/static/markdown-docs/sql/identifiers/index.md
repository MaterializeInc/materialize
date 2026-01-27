# Identifiers
SQL identifiers are names of columns and database objects such as sources and views.
In Materialize, identifiers are used to refer to columns and database objects
like sources, views, and indexes.

## Naming restrictions

Materialize has the following naming restrictions for identifiers:

| Position            | Allowed Characters                                                                 |
|---------------------|------------------------------------------------------------------------------------|
| **First character** | ASCII letters (`a`-`z`, `A`-`Z`), underscore (`_`), or any non-ASCII character     |
| **Remaining**       | ASCII letters (`a`-`z`, `A`-`Z`), digits (`0`-`9`), underscores (`_`), dollar sign (`$`), or any non-ASCII character |

To override these restrictions, you can enclose the identifier in double quotes;
e.g., `"123_source"` or `"fun_source_@"`. Inside double quotes, characters are interpreted literally, except for the double-quote character itself. To include a double quote within a double-quoted identifier, escape it by writing two adjacent double quotes, as in "includes""quote".

> **Note:** The identifiers `"."` and `".."` are not allowed.


## Case sensitivity

Materialize performs case folding (the caseless comparison of text) for identifiers, which means that identifiers are effectively case-insensitive (`foo` is the same as `FOO` is the same as `fOo`). This can cause issues when column names come from data sources which do support case-sensitive names, such as Avro-formatted sources.

To avoid conflicts, double-quote all field names (`"field_name"`) when working with case-sensitive sources.

## Renaming restrictions

You cannot rename an item if any of the following are true:

- **It is not uniquely qualified across all dependent references.**

  For example, suppose you have:

  - Two views named `v1` in different databases (`d1` and `d2`) under the same schema name (`s1`), and
  - Both `v1` views are referenced by another view.

  You may rename either `v1` only if every dependent query that mentions both views **fully qualifies all such references**, e.g.:

  ```mzsql
  CREATE VIEW v2 AS
  SELECT *
  FROM d1.s1.v1
  JOIN d2.s1.v1
  ON d1.s1.v1.a = d2.s1.v1.a;
  ```

  If the two views were instead in schemas with distinct names, qualifying by schema alone would be sufficient (you would not need to include the database name).

- Renaming would cause any identifier collision with a dependent query.

  - An existing collision: a dependent query already uses the item’s current
    identifier for some database, schema, object, or column, so changing the
    item’s name would change how that identifier resolves.

    In the examples below, v1 cannot be renamed because dependent queries already use the identifier v1:

    - Any dependent query references a database, schema, or column that uses the same identifier.

        In the following examples, `v1` could _not_ be renamed:

        ```mzsql
        CREATE VIEW v3 AS
        SELECT *
        FROM v1
        JOIN v2
        ON v1.a = v2.v1
        ```

        ```mzsql
        CREATE VIEW v4 AS
        SELECT *
        FROM v1
        JOIN v1.v2
        ON v1.a = v2.a
        ```

  - A proposed-name collision: the new name matches any identifier referenced in
    a dependent query, whether that identifier is referenced explicitly or
    implicitly.

    Consider this example:

    ```mzsql
    CREATE VIEW v5 AS
    SELECT *
    FROM d1.s1.v2
    JOIN v1
    ON v1.a = v2.b
    ```

    You could not rename `v1` to:

    - `a`
    - `b`
    - `v2`
    - `s1`
    - `d1`
    - `materialize` or `public` (implicitly referenced by `materialize.public.v1` using the default database and schema)

## Keyword collision

Materialize is very permissive with accepting SQL keywords as identifiers (e.g.
`offset`, `user`). If Materialize cannot use a keyword as an
identifier in a particular location, it throws a syntax error. You can wrap the
identifier in double quotes to force Materialize to interpret the word as an
identifier instead of a keyword.

For example, `SELECT offset` is invalid, because it looks like a mistyping of
`SELECT OFFSET <n>`. You can wrap the identifier in double quotes, as in
`SELECT "offset"`, to resolve the error.

We recommend that you avoid using keywords as identifiers whenever possible, as
the syntax errors that result are not always obvious.

The current keywords are listed below.

| | | | |
|--|--|--|--||`ABORT` |`ACCESS` |`ACTION` |`ADD`||`ADDED` |`ADDRESS` |`ADDRESSES` |`AFTER`||`AGGREGATE` |`AGGREGATION` |`ALIGNED` |`ALL`||`ALTER` |`ANALYSE` |`ANALYSIS` |`ANALYZE`||`AND` |`ANY` |`APPLY` |`ARITY`||`ARN` |`ARRANGED` |`ARRANGEMENT` |`ARRAY`||`AS` |`ASC` |`ASSERT` |`ASSUME`||`AT` |`AUCTION` |`AUTHORITY` |`AVAILABILITY`||`AVRO` |`AWS` |`BATCH` |`BEGIN`||`BETWEEN` |`BIGINT` |`BILLED` |`BODY`||`BOOLEAN` |`BOTH` |`BPCHAR` |`BROKEN`||`BROKER` |`BROKERS` |`BY` |`BYTES`||`CAPTURE` |`CARDINALITY` |`CASCADE` |`CASE`||`CAST` |`CATALOG` |`CERTIFICATE` |`CHAIN`||`CHAINS` |`CHAR` |`CHARACTER` |`CHARACTERISTICS`||`CHECK` |`CLASS` |`CLIENT` |`CLOCK`||`CLOSE` |`CLUSTER` |`CLUSTERS` |`COALESCE`||`COLLATE` |`COLUMN` |`COLUMNS` |`COMMENT`||`COMMIT` |`COMMITTED` |`COMPACTION` |`COMPATIBILITY`||`COMPRESSION` |`COMPUTE` |`COMPUTECTL` |`CONFIG`||`CONFLUENT` |`CONNECTION` |`CONNECTIONS` |`CONSTRAINT`||`CONTINUAL` |`COPY` |`COUNT` |`COUNTER`||`CPU` |`CREATE` |`CREATECLUSTER` |`CREATEDB`||`CREATENETWORKPOLICY` |`CREATEROLE` |`CREATION` |`CREDENTIAL`||`CROSS` |`CSE` |`CSV` |`CURRENT`||`CURSOR` |`DATABASE` |`DATABASES` |`DATUMS`||`DAY` |`DAYS` |`DEALLOCATE` |`DEBEZIUM`||`DEBUG` |`DEBUGGING` |`DEC` |`DECIMAL`||`DECLARE` |`DECODING` |`DECORRELATED` |`DEFAULT`||`DEFAULTS` |`DELETE` |`DELIMITED` |`DELIMITER`||`DELTA` |`DESC` |`DETAILS` |`DIRECTION`||`DISCARD` |`DISK` |`DISTINCT` |`DOC`||`DOT` |`DOUBLE` |`DROP` |`EAGER`||`ELEMENT` |`ELSE` |`ENABLE` |`END`||`ENDPOINT` |`ENFORCED` |`ENVELOPE` |`EQUIVALENCES`||`ERROR` |`ERRORS` |`ESCAPE` |`ESTIMATE`||`EVERY` |`EXCEPT` |`EXCLUDE` |`EXECUTE`||`EXISTS` |`EXPECTED` |`EXPLAIN` |`EXPOSE`||`EXPRESSIONS` |`EXTERNAL` |`EXTRACT` |`FACTOR`||`FALSE` |`FAST` |`FEATURES` |`FETCH`||`FIELDS` |`FILE` |`FILES` |`FILTER`||`FIRST` |`FIXPOINT` |`FLOAT` |`FOLLOWING`||`FOR` |`FOREIGN` |`FORMAT` |`FORWARD`||`FROM` |`FULL` |`FULLNAME` |`FUNCTION`||`FUSION` |`GENERATOR` |`GRANT` |`GREATEST`||`GROUP` |`GROUPS` |`HAVING` |`HEADER`||`HEADERS` |`HINTS` |`HISTORY` |`HOLD`||`HOST` |`HOUR` |`HOURS` |`HUMANIZED`||`HYDRATION` |`ICEBERG` |`ID` |`IDENTIFIERS`||`IDS` |`IF` |`IGNORE` |`ILIKE`||`IMPLEMENTATIONS` |`IMPORTED` |`IN` |`INCLUDE`||`INDEX` |`INDEXES` |`INFO` |`INHERIT`||`INLINE` |`INNER` |`INPUT` |`INSERT`||`INSIGHTS` |`INSPECT` |`INSTANCE` |`INT`||`INTEGER` |`INTERNAL` |`INTERSECT` |`INTERVAL`||`INTO` |`INTROSPECTION` |`IS` |`ISNULL`||`ISOLATION` |`JOIN` |`JOINS` |`JSON`||`KAFKA` |`KEY` |`KEYS` |`LAST`||`LATERAL` |`LATEST` |`LEADING` |`LEAST`||`LEFT` |`LEGACY` |`LETREC` |`LEVEL`||`LIKE` |`LIMIT` |`LINEAR` |`LIST`||`LOAD` |`LOCAL` |`LOCALLY` |`LOG`||`LOGICAL` |`LOGIN` |`LOWERING` |`MANAGED`||`MANUAL` |`MAP` |`MARKETING` |`MATERIALIZE`||`MATERIALIZED` |`MAX` |`MECHANISMS` |`MEMBERSHIP`||`MEMORY` |`MESSAGE` |`METADATA` |`MINUTE`||`MINUTES` |`MODE` |`MONTH` |`MONTHS`||`MUTUALLY` |`MYSQL` |`NAME` |`NAMES`||`NAMESPACE` |`NATURAL` |`NEGATIVE` |`NETWORK`||`NEW` |`NEXT` |`NFC` |`NFD`||`NFKC` |`NFKD` |`NO` |`NOCREATECLUSTER`||`NOCREATEDB` |`NOCREATEROLE` |`NODE` |`NOINHERIT`||`NOLOGIN` |`NON` |`NONE` |`NORMALIZE`||`NOSUPERUSER` |`NOT` |`NOTICE` |`NOTICES`||`NULL` |`NULLIF` |`NULLS` |`OBJECTS`||`OF` |`OFFSET` |`ON` |`ONLY`||`OPERATOR` |`OPTIMIZED` |`OPTIMIZER` |`OPTIONS`||`OR` |`ORDER` |`ORDINALITY` |`OUTER`||`OVER` |`OWNED` |`OWNER` |`PARTITION`||`PARTITIONS` |`PASSWORD` |`PATH` |`PATTERN`||`PHYSICAL` |`PLAN` |`PLANS` |`POLICIES`||`POLICY` |`PORT` |`POSITION` |`POSTGRES`||`PRECEDING` |`PRECISION` |`PREFIX` |`PREPARE`||`PRIMARY` |`PRIORITIZE` |`PRIVATELINK` |`PRIVILEGES`||`PROGRESS` |`PROJECTION` |`PROTOBUF` |`PROTOCOL`||`PUBLIC` |`PUBLICATION` |`PUSHDOWN` |`QUALIFY`||`QUERY` |`QUOTE` |`RAISE` |`RANGE`||`RATE` |`RAW` |`READ` |`READY`||`REAL` |`REASSIGN` |`RECURSION` |`RECURSIVE`||`REDACTED` |`REDUCE` |`REFERENCE` |`REFERENCES`||`REFRESH` |`REGEX` |`REGION` |`REGISTRY`||`RELATION` |`RENAME` |`REOPTIMIZE` |`REPEATABLE`||`REPLACE` |`REPLACEMENT` |`REPLAN` |`REPLICA`||`REPLICAS` |`REPLICATION` |`RESET` |`RESPECT`||`RESTRICT` |`RETAIN` |`RETURN` |`RETURNING`||`REVOKE` |`RIGHT` |`ROLE` |`ROLES`||`ROLLBACK` |`ROTATE` |`ROUNDS` |`ROW`||`ROWS` |`RULES` |`SASL` |`SCALE`||`SCHEDULE` |`SCHEMA` |`SCHEMAS` |`SCOPE`||`SECOND` |`SECONDS` |`SECRET` |`SECRETS`||`SECURITY` |`SEED` |`SELECT` |`SEQUENCES`||`SERIALIZABLE` |`SERVER` |`SERVICE` |`SESSION`||`SET` |`SHARD` |`SHOW` |`SINK`||`SINKS` |`SIZE` |`SKEW` |`SMALLINT`||`SNAPSHOT` |`SOME` |`SOURCE` |`SOURCES`||`SQL` |`SSH` |`SSL` |`START`||`STDIN` |`STDOUT` |`STORAGE` |`STORAGECTL`||`STRATEGY` |`STRICT` |`STRING` |`STRONG`||`SUBSCRIBE` |`SUBSOURCE` |`SUBSOURCES` |`SUBSTRING`||`SUBTREE` |`SUPERUSER` |`SWAP` |`SYNTAX`||`SYSTEM` |`TABLE` |`TABLES` |`TAIL`||`TASK` |`TASKS` |`TEMP` |`TEMPORARY`||`TEXT` |`THEN` |`TICK` |`TIES`||`TIME` |`TIMEOUT` |`TIMESTAMP` |`TIMESTAMPTZ`||`TIMING` |`TO` |`TOKEN` |`TOPIC`||`TPCH` |`TRACE` |`TRAILING` |`TRANSACTION`||`TRANSACTIONAL` |`TRANSFORM` |`TRIM` |`TRUE`||`TUNNEL` |`TYPE` |`TYPES` |`UNBOUNDED`||`UNCOMMITTED` |`UNION` |`UNIQUE` |`UNKNOWN`||`UNNEST` |`UNTIL` |`UP` |`UPDATE`||`UPSERT` |`URL` |`USAGE` |`USER`||`USERNAME` |`USERS` |`USING` |`VALIDATE`||`VALUE` |`VALUES` |`VARCHAR` |`VARIADIC`||`VARYING` |`VERBOSE` |`VERSION` |`VIEW`||`VIEWS` |`WAIT` |`WAREHOUSE` |`WARNING`||`WEBHOOK` |`WHEN` |`WHERE` |`WHILE`||`WINDOW` |`WIRE` |`WITH` |`WITHIN`||`WITHOUT` |`WORK` |`WORKERS` |`WORKLOAD`||`WRITE` |`YEAR` |`YEARS` |`ZONE`||`ZONES` |&nbsp; |&nbsp; |&nbsp;|
