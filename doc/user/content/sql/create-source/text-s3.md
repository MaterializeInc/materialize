---
title: "CREATE SOURCE: Text or bytes from an S3 bucket"
description: "Learn how to connect Materialize to an S3 Bucket"
menu:
  main:
    parent: 'create-source'
---
{{% create-source/intro %}}
This document details how to connect Materialize to an S3 Bucket that contains
multiple objects, and to listen for new object creation. Each S3 object can
contain multiple byte- or text-encoded records, separated by newlines.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-s3-text.svg" >}}

#### `key_constraint`

{{< diagram "key-constraint.svg" >}}

#### `with_options`

{{< diagram "with-options-aws.svg" >}}

{{% create-source/syntax-details connector="s3" formats="regex text bytes" envelopes="append-only" keyConstraint=true %}}

## Examples

Assuming there is an S3 bucket named "frontend" that contains the following keys and associated
content:

**logs/2020/12/31/frontend.log**
```text
99.99.44.44 - - [12/31/2020:23:55:59 +0000] "GET /updates HTTP/1.1" 200 10020 "-" "Python/Requests 22"
80.91.33.133 - - [12/31/2020:23:55:02 +0000] "GET /downloads/materialized HTTP/1.1" 304 0 "-" "Rust/reqwest 0.3"
173.203.139.108 - - [12/31/2020:23:55:07 +0000] "GET /wpadmin HTTP/1.1" 404 332 "-" "Firefox 9000"
173.203.139.108 - - [12/31/2020:23:55:14 +0000] "GET /downloads/materialized HTTP/1.1" 404 334 "-" "Python/Requests 22"
99.99.44.44 - - [12/31/2020:23:55:01 +0000] "GET /downloads/materialized HTTP/1.1" 304 0 "-" "Python/Requests 22"
80.91.33.133 - - [12/31/2020:23:55:41 +0000] "GET /downloads/materialized HTTP/1.1" 304 0 "-" "Rust/reqwest 0.3"
37.26.93.214 - - [12/31/2020:23:55:52 +0000] "GET /updates HTTP/1.1" 200 3318 "-" "Go 1.1 package http"
```

**logs/2021/01/01/frontend.log**
```text
99.99.44.44 - - [01/01/2021:00:00:41 +0000] "GET /updates HTTP/1.1" 200 10020 "-" "Python/Requests 22"
188.138.60.101 - - [01/01/2021:00:00:48 +0000] "GET /updates HTTP/1.1" 200 10020 "-" "Python/Requests 22"
46.50.21.83 - - [01/01/2021:00:00:02 +0000] "GET /downloads/materialized HTTP/1.1" 304 0 "-" "Python/Requests 22.01"
99.99.44.44 - - [01/01/2021:00:00:25 +0000] "GET /downloads/materialized HTTP/1.1" 304 0 "-" "Python/Requests 22"
91.239.186.133 - - [01/01/2021:00:00:04 +0000] "GET /updates HTTP/1.1" 200 10020 "-" "Python/Requests 22"
173.203.139.108 - - [01/01/2021:00:00:08 +0000] "GET /downloads/materialized HTTP/1.1" 304 0 "-" "Python/Requests 22"
80.91.33.133 - - [01/01/2021:00:00:04 +0000] "GET /downloads/materialized HTTP/1.1" 304 0 "-" "Rust/reqwest 0.3"
93.190.71.150 - - [01/01/2021:00:00:33 +0000] "GET /updates HTTP/1.1" 200 10020 "-" "Python/Requests 22"
91.234.194.89 - - [01/01/2021:00:00:57 +0000] "GET /updates HTTP/1.1" 200 10020 "-" "Python/Requests 22"
46.4.83.163 - - [01/01/2021:00:00:20 +0000] "GET /downloads/materialized HTTP/1.1" 304 0 "-" "Python/Requests 22"
173.203.139.108 - - [01/01/2021:00:00:39 +0000] "GET /downloads/materialized HTTP/1.1" 404 335 "-" "Python/Requests 22"
```

### Example TEXT format

We'll create a source that ingests all these logs, so that we can execute some
quick and dirty analysis:

```sql
CREATE MATERIALIZED SOURCE frontend_logs
FROM S3 DISCOVER OBJECTS MATCHING 'logs/202?/**/*.log' USING BUCKET SCAN 'frontend'
WITH (region = 'us-east-2')
FORMAT TEXT;
```

With that, we will have one SQL row per line in both files, so we can for example query
for all the lines that match `updates`, ordered by position that we encountered the line:

```sql
> SELECT mz_record, text FROM frontend_logs WHERE text LIKE '%updates%' ORDER BY mz_record;
 mz_record |                                     text
 1         | 99.99.44.44 - - [12/31/2020:23:55:59] "GET /updates HTTP/1.1" 200 10020 "-" "Python/Requests_22"
 7         | 37.26.93.214 - - [12/31/2020:23:55:52] "GET /updates HTTP/1.1" 200 3318 "-" "Go_1.1_package_http"
 8         | 99.99.44.44 - - [01/01/2021:00:00:41] "GET /updates HTTP/1.1" 200 10020 "-" "Python/Requests_22"
 9         | 188.138.60.101 - - [01/01/2021:00:00:48] "GET /updates HTTP/1.1" 200 10020 "-" "Python/Requests_22"
 12        | 91.239.186.133 - - [01/01/2021:00:00:04] "GET /updates HTTP/1.1" 200 10020 "-" "Python/Requests_22"
 15        | 93.190.71.150 - - [01/01/2021:00:00:33] "GET /updates HTTP/1.1" 200 10020 "-" "Python/Requests_22"
 16        | 91.234.194.89 - - [01/01/2021:00:00:57] "GET /updates HTTP/1.1" 200 10020 "-" "Python/Requests_22"
```

### Example REGEX format

It's generally more convenient to work with well-structured columnar data, though, so we
can use the REGEX format specifier to extract useful data:

```sql
CREATE MATERIALIZED SOURCE frontend_logs
FROM S3 DISCOVER OBJECTS MATCHING 'logs/202?/**/*.log' USING BUCKET SCAN 'frontend'
WITH (region = 'us-east-2')
FORMAT REGEX '(?P<ip>[^ ]+) - - \[?P<dt>([^]]_)\] "(?P<method>\w+) (?P<path>[^ ]+)[^"]+" (?P<status>\d+) (?P<content_length>\d+) "-" "(?P<user_agent>[^"]+)"';
```

With that, we will have one SQL row per line in both files, so we can for example query
for all the lines that have `/updates` as their exact path:

```sql
> SELECT dt, ip, user_agent FROM frontend_logs WHERE path = '/updates';
       dt           |      ip        |    user_agent
01/01/2021:00:00:04 | 91.239.186.133 | Python/Requests 22
01/01/2021:00:00:33 | 93.190.71.150  | Python/Requests 22
01/01/2021:00:00:41 | 99.99.44.44    | Python/Requests 22
01/01/2021:00:00:48 | 188.138.60.101 | Python/Requests 22
01/01/2021:00:00:57 | 91.234.194.89  | Python/Requests 22
12/31/2020:23:55:52 | 37.26.93.214   | Go 1.1 package http
12/31/2020:23:55:59 | 99.99.44.44    | Python/Requests 22
```

### Example Single Object

When creating a source that ingests a single object, we only require the `ListObject`
[permission](https://materialize.com/docs/sql/create-source/text-s3/#permissions-required):

```sql
CREATE MATERIALIZED SOURCE single_object
FROM S3 DISCOVER OBJECTS MATCHING 'logs/2020/12/31/frontend.log' USING BUCKET SCAN 'frontend'
WITH (region = 'us-east-2')
FORMAT TEXT;
```


## Related Pages

- S3 with [`CSV`](../csv-s3)/[`JSON`](../json-s3) encoded data
- [`CREATE SOURCE`](../)
- [`CREATE VIEW`](../../create-view)
- [`SELECT`](../../select)
