# SQL functions & operators

Learn more about the SQL functions and operators supported in Materialize



This page details Materialize's supported SQL [functions](#functions) and [operators](#operators).

## Functions

### Unmaterializable functions

Several functions in Materialize are **unmaterializable** because their output
depends upon state besides their input parameters, like the value of a session
parameter or the timestamp of the current transaction. You cannot create an
[index](/sql/create-index) or materialized view that depends on an
unmaterializable function, but you can use them in non-materialized views and
one-off [`SELECT`](/sql/select) statements.

Unmaterializable functions are marked as such in the table below.

### Side-effecting functions

Several functions in Materialize are **side-effecting** because their evaluation
changes system state. For example, the `pg_cancel_backend` function allows
canceling a query running on another connection.

Materialize offers only limited support for these functions. They may be called
only at the top level of a `SELECT` statement, like so:

```mzsql
SELECT side_effecting_function(arg, ...);
```

You cannot manipulate or alias the function call expression, call multiple
side-effecting functions in the same `SELECT` statement, nor add any additional
clauses to the `SELECT` statement (e.g., `FROM`, `WHERE`).

Side-effecting functions are marked as such in the table below.

### Generic functionsGeneric functions can typically take arguments of any type.#### `CAST (cast_expr) -> T`

Value as type <code>T</code> [(docs)](/sql/functions/cast)#### `coalesce(x: T...) -> T?`

First non-<em>NULL</em> arg, or <em>NULL</em> if all are <em>NULL</em>.#### `greatest(x: T...) -> T?`

The maximum argument, or <em>NULL</em> if all are <em>NULL</em>.#### `least(x: T...) -> T?`

The minimum argument, or <em>NULL</em> if all are <em>NULL</em>.#### `nullif(x: T, y: T) -> T?`

<em>NULL</em> if <code>x == y</code>, else <code>x</code>.### Aggregate functionsAggregate functions take one or more of the same element type as arguments.#### `array_agg(x: T) -> T[]`

Aggregate values (including nulls) as an array [(docs)](/sql/functions/array_agg)#### `avg(x: T) -> U`

<p>Average of <code>T</code>&rsquo;s values.</p>
<p>Returns <code>numeric</code> if <code>x</code> is <code>int</code>, <code>double</code> if <code>x</code> is <code>real</code>, else returns
same type as <code>x</code>.</p>
#### `bool_and(x: T) -> T`

<em>NULL</em> if all values of <code>x</code> are <em>NULL</em>, otherwise true if all values of <code>x</code> are true, otherwise false.#### `bool_or(x: T) -> T`

<em>NULL</em> if all values of <code>x</code> are <em>NULL</em>, otherwise true if any values of <code>x</code> are true, otherwise false.#### `count(x: T) -> bigint`

Number of non-<em>NULL</em> inputs.#### `jsonb_agg(expression) -> jsonb`

Aggregate values (including nulls) as a jsonb array [(docs)](/sql/functions/jsonb_agg)#### `jsonb_object_agg(keys, values) -> jsonb`

Aggregate keys and values (including nulls) as a jsonb object [(docs)](/sql/functions/jsonb_object_agg)#### `max(x: T) -> T`

Maximum value among <code>T</code>.#### `min(x: T) -> T`

Minimum value among <code>T</code>.#### `stddev(x: T) -> U`

<p>Historical alias for <code>stddev_samp</code>. <em>(imprecise)</em></p>
<p>Returns <code>numeric</code> if <code>x</code> is <code>int</code>, <code>double</code> if <code>x</code> is <code>real</code>, else returns
same type as <code>x</code>.</p>
#### `stddev_pop(x: T) -> U`

<p>Population standard deviation of <code>T</code>&rsquo;s values. <em>(imprecise)</em></p>
<p>Returns <code>numeric</code> if <code>x</code> is <code>int</code>, <code>double</code> if <code>x</code> is <code>real</code>, else returns
same type as <code>x</code>.</p>
#### `stddev_samp(x: T) -> U`

<p>Sample standard deviation of <code>T</code>&rsquo;s values. <em>(imprecise)</em></p>
<p>Returns <code>numeric</code> if <code>x</code> is <code>int</code>, <code>double</code> if <code>x</code> is <code>real</code>, else returns
same type as <code>x</code>.</p>
#### `string_agg(value: text, delimiter: text) -> text`

Concatenates the non-null input values into text. Each value after the first is preceded by the corresponding delimiter [(docs)](/sql/functions/string_agg)#### `sum(x: T) -> U`

<p>Sum of <code>T</code>&rsquo;s values</p>
<p>Returns <code>bigint</code> if <code>x</code> is <code>int</code> or <code>smallint</code>, <code>numeric</code> if <code>x</code> is <code>bigint</code> or <code>uint8</code>,
<code>uint8</code> if <code>x</code> is <code>uint4</code> or <code>uint2</code>, else returns same type as <code>x</code>.</p>
#### `variance(x: T) -> U`

<p>Historical alias for <code>var_samp</code>. <em>(imprecise)</em></p>
<p>Returns <code>numeric</code> if <code>x</code> is <code>int</code>, <code>double</code> if <code>x</code> is <code>real</code>, else returns
same type as <code>x</code>.</p>
#### `var_pop(x: T) -> U`

<p>Population variance of <code>T</code>&rsquo;s values. <em>(imprecise)</em></p>
<p>Returns <code>numeric</code> if <code>x</code> is <code>int</code>, <code>double</code> if <code>x</code> is <code>real</code>, else returns
same type as <code>x</code>.</p>
#### `var_samp(x: T) -> U`

<p>Sample variance of <code>T</code>&rsquo;s values. <em>(imprecise)</em></p>
<p>Returns <code>numeric</code> if <code>x</code> is <code>int</code>, <code>double</code> if <code>x</code> is <code>real</code>, else returns
same type as <code>x</code>.</p>
### List functionsList functions take <a href="../types/list" ><code>list</code></a> arguments, and are <a href="../types/list/#polymorphism" >polymorphic</a>.#### `list_agg(x: any) -> L`

Aggregate values (including nulls) as a list [(docs)](/sql/functions/list_agg)#### `list_append(l: listany, e: listelementany) -> L`

Appends <code>e</code> to <code>l</code>.#### `list_cat(l1: listany, l2: listany) -> L`

Concatenates <code>l1</code> and <code>l2</code>.#### `list_length(l: listany) -> int`

Return the number of elements in <code>l</code>.#### `list_prepend(e: listelementany, l: listany) -> listany`

Prepends <code>e</code> to <code>l</code>.### Map functionsMap functions take <a href="../types/map" ><code>map</code></a> arguments, and are <a href="../types/#polymorphism" >polymorphic</a>.#### `map_length(m: mapany) -> int`

Return the number of elements in <code>m</code>.#### `map_build(kvs: list record(text, T)) -> map[text=>T]`

Builds a map from a list of records whose fields are two elements, the
first of which is <code>text</code>. In the face of duplicate keys, <code>map_build</code> retains
value from the record in the latest positition. This function is
purpose-built to process <a href="/sql/create-source/kafka/#headers" >Kafka headers</a>.#### `map_agg(keys: text, values: T) -> map[text=>T]`

Aggregate keys and values (including nulls) as a map [(docs)](/sql/functions/map_agg)### Numbers functionsNumber functions take number-like arguments, e.g. <a href="../types/int" ><code>int</code></a>, <a href="../types/float" ><code>float</code></a>, <a href="../types/numeric" ><code>numeric</code></a>, unless otherwise specified.#### `abs(x: N) -> N`

The absolute value of <code>x</code>.#### `cbrt(x: double precision) -> double precision`

The cube root of <code>x</code>.#### `ceil(x: N) -> double precision`

The smallest integer &gt;= <code>x</code>.#### `ceiling(x: N) -> double precision`

Alias of <code>ceil</code>.#### `exp(x: N) -> double precision`

Exponential of <code>x</code> (e raised to the given power)#### `floor(x: N) -> double precision`

The largest integer &lt;= <code>x</code>.#### `ln(x: double precision) -> double precision`

Natural logarithm of <code>x</code>.#### `ln(x: numeric) -> numeric`

Natural logarithm of <code>x</code>.#### `log(x: double precision) -> double precision`

Base 10 logarithm of <code>x</code>.#### `log(x: numeric) -> numeric`

Base 10 logarithm of <code>x</code>.#### `log10(x: double precision) -> double precision`

Base 10 logarithm of <code>x</code>, same as <code>log</code>.#### `log10(x: numeric) -> numeric`

Base 10 logarithm of <code>x</code>, same as <code>log</code>.#### `log(b: numeric, x: numeric) -> numeric`

Base <code>b</code> logarithm of <code>x</code>.#### `mod(x: N, y: N) -> N`

<code>x % y</code>#### `pow(x: double precision, y: double precision) -> double precision`

Alias of <code>power</code>.#### `pow(x: numeric, y: numeric) -> numeric`

Alias of <code>power</code>.#### `power(x: double precision, y: double precision) -> double precision`

<code>x</code> raised to the power of <code>y</code>.#### `power(x: numeric, y: numeric) -> numeric`

<code>x</code> raised to the power of <code>y</code>.#### `round(x: N) -> double precision`

<code>x</code> rounded to the nearest whole number.
If <code>N</code> is <code>real</code> or <code>double precision</code>, rounds ties to the nearest even number.
If <code>N</code> is <code>numeric</code>, rounds ties away from zero.#### `round(x: numeric, y: int) -> numeric`

<code>x</code> rounded to <code>y</code> decimal places, while retaining the same <a href="../types/numeric" ><code>numeric</code></a> scale; rounds ties away from zero.#### `sqrt(x: numeric) -> numeric`

The square root of <code>x</code>.#### `sqrt(x: double precision) -> double precision`

The square root of <code>x</code>.#### `trunc(x: N) -> double precision`

<code>x</code> truncated toward zero to a whole number.### Trigonometric functionsTrigonometric functions take and return <code>double precision</code> values.#### `cos(x: double precision) -> double precision`

The cosine of <code>x</code>, with <code>x</code> in radians.#### `acos(x: double precision) -> double precision`

The inverse cosine of <code>x</code>, result in radians.#### `cosh(x: double precision) -> double precision`

The hyperbolic cosine of <code>x</code>, with <code>x</code> as a hyperbolic angle.#### `acosh(x: double precision) -> double precision`

The inverse hyperbolic cosine of <code>x</code>.#### `cot(x: double precision) -> double precision`

The cotangent of <code>x</code>, with <code>x</code> in radians.#### `sin(x: double precision) -> double precision`

The sine of <code>x</code>, with <code>x</code> in radians.#### `asin(x: double precision) -> double precision`

The inverse sine of <code>x</code>, result in radians.#### `sinh(x: double precision) -> double precision`

The hyperbolic sine of <code>x</code>, with <code>x</code> as a hyperbolic angle.#### `asinh(x: double precision) -> double precision`

The inverse hyperbolic sine of <code>x</code>.#### `tan(x: double precision) -> double precision`

The tangent of <code>x</code>, with <code>x</code> in radians.#### `atan(x: double precision) -> double precision`

The inverse tangent of <code>x</code>, result in radians.#### `tanh(x: double precision) -> double precision`

The hyperbolic tangent of <code>x</code>, with <code>x</code> as a hyperbolic angle.#### `atanh(x: double precision) -> double precision`

The inverse hyperbolic tangent of <code>x</code>.#### `radians(x: double precision) -> double precision`

Converts degrees to radians.#### `degrees(x: double precision) -> double precision`

Converts radians to degrees.### String functions#### `ascii(s: str) -> int`

The ASCII value of <code>s</code>&rsquo;s left-most character.#### `btrim(s: str) -> str`

Trim all spaces from both sides of <code>s</code>.#### `btrim(s: str, c: str) -> str`

Trim any character in <code>c</code> from both sides of <code>s</code>.#### `bit_count(b: bytea) -> bigint`

Returns the number of bits set in the bit string (aka <em>popcount</em>).#### `bit_length(s: str) -> int`

Number of bits in <code>s</code>.#### `bit_length(b: bytea) -> int`

Number of bits in <code>b</code>.#### `char_length(s: str) -> int`

Number of code points in <code>s</code>.#### `chr(i: int) -> str`

Character with the given Unicode codepoint.
Only supports codepoints that can be encoded in UTF-8.
The NULL (0) character is not allowed.#### `concat(f: any, r: any...) -> text`

Concatenates the text representation of non-NULL arguments. The maximum length of the result string is 100 MiB.#### `concat_ws(sep: str, f: any, r: any...) -> text`

Concatenates the text representation of non-NULL arguments from <code>f</code> and <code>r</code> separated by <code>sep</code>. The maximum length of the result string is 100 MiB.#### `convert_from(b: bytea, src_encoding: text) -> text`

Convert data <code>b</code> from original encoding specified by <code>src_encoding</code> into <code>text</code>.#### `decode(s: text, format: text) -> bytea`

Decode <code>s</code> using the specified textual representation. The maximum size of the result is 100 MiB. [(docs)](/sql/functions/encode)#### `encode(b: bytea, format: text) -> text`

Encode <code>b</code> using the specified textual representation [(docs)](/sql/functions/encode)#### `get_bit(b: bytea, n: int) -> int`

Return the <code>n</code>th bit from <code>b</code>, where the left-most bit in <code>b</code> is at the 0th position.#### `get_byte(b: bytea, n: int) -> int`

Return the <code>n</code>th byte from <code>b</code>, where the left-most byte in <code>b</code> is at the 0th position.#### `constant_time_eq(a: bytea, b: bytea) -> bool`

Returns <code>true</code> if the arrays are identical, otherwise returns <code>false</code>. The implementation mitigates timing attacks by making a best-effort attempt to execute in constant time if the arrays have the same length, regardless of their contents.#### `constant_time_eq(a: text, b: text) -> bool`

Returns <code>true</code> if the strings are identical, otherwise returns <code>false</code>. The implementation mitigates timing attacks by making a best-effort attempt to execute in constant time if the strings have the same length, regardless of their contents.#### `initcap(a: text) -> text`

Returns <code>a</code> with the first character of every word in upper case and all
other characters in lower case. Words are separated by non-alphanumeric
characters.#### `left(s: str, n: int) -> str`

The first <code>n</code> characters of <code>s</code>. If <code>n</code> is negative, all but the last <code>|n|</code> characters of <code>s</code>.#### `length(s: str) -> int`

Number of code points in <code>s</code>. [(docs)](/sql/functions/length)#### `length(b: bytea) -> int`

Number of bytes in <code>s</code>. [(docs)](/sql/functions/length)#### `length(s: bytea, encoding_name: str) -> int`

Number of code points in <code>s</code> after encoding [(docs)](/sql/functions/length)#### `lower(s: str) -> str`

Convert <code>s</code> to lowercase.#### `lpad(s: str, len: int) -> str`

Prepend <code>s</code> with spaces up to length <code>len</code>, or right truncate if <code>len</code> is less than the length of <code>s</code>. The maximum length of the result string is 100 MiB.#### `lpad(s: str, len: int, p: str) -> str`

Prepend <code>s</code> with characters pulled from <code>p</code> up to length <code>len</code>, or right truncate if <code>len</code> is less than the length of <code>s</code>. The maximum length of the result string is 100 MiB.#### `ltrim(s: str) -> str`

Trim all spaces from the left side of <code>s</code>.#### `ltrim(s: str, c: str) -> str`

Trim any character in <code>c</code> from the left side of <code>s</code>.#### `normalize(s: str) -> str`

Normalize <code>s</code> to Unicode Normalization Form NFC (default). [(docs)](/sql/functions/normalize)#### `normalize(s: str, form: keyword) -> str`

Normalize <code>s</code> to the specified Unicode normalization form (NFC, NFD, NFKC, or NFKD). [(docs)](/sql/functions/normalize)#### `octet_length(s: str) -> int`

Number of bytes in <code>s</code>.#### `octet_length(b: bytea) -> int`

Number of bytes in <code>b</code>.#### `parse_ident(ident: str[, strict_mode: bool]) -> str[]`

Given a qualified identifier like <code>a.&quot;b&quot;.c</code>, splits into an array of the
constituent identifiers with quoting removed and escape sequences decoded.
Extra characters after the last identifier are ignored unless the
<code>strict_mode</code> parameter is <code>true</code> (defaults to <code>false</code>).#### `position(sub: str IN s: str) -> int`

The starting index of <code>sub</code> within <code>s</code> or <code>0</code> if <code>sub</code> is not a substring of <code>s</code>.#### `regexp_match(haystack: str, needle: str [, flags: str]]) -> str[]`

Matches the regular expression <code>needle</code> against haystack, returning a
string array that contains the value of each capture group specified in
<code>needle</code>, in order. If <code>flags</code> is set to the string <code>i</code> matches
case-insensitively.#### `regexp_replace(source: str, pattern: str, replacement: str [, flags: str]]) -> str`

> **Warning:** This function has the potential to produce very large strings and
> may cause queries to run out of memory or crash. Use with caution.

<p>Replaces the first occurrence of <code>pattern</code> with <code>replacement</code> in <code>source</code>.
No match will return <code>source</code> unchanged.</p>
<p>If <code>flags</code> is set to <code>g</code>, all occurrences are replaced.
If <code>flags</code> is set to <code>i</code>, matches case-insensitively.</p>
<p><code>$N</code> or <code>$name</code> in <code>replacement</code> can be used to match capture groups.
<code>${N}</code> must be used to disambiguate capture group indexes from names if other characters follow <code>N</code>.
A <code>$$</code> in <code>replacement</code> will write a literal <code>$</code>.</p>
<p>See the <a href="https://docs.rs/regex/latest/regex/struct.Regex.html#method.replace" >rust regex docs</a> for more details about replacement.</p>
#### `regexp_matches(haystack: str, needle: str [, flags: str]]) -> str[]`

Matches the regular expression <code>needle</code> against haystack, returning a
string array that contains the value of each capture group specified in
<code>needle</code>, in order. If <code>flags</code> is set to the string <code>i</code> matches
case-insensitively. If <code>flags</code> is set to the string <code>g</code> all matches are
returned, otherwise only the first match is returned. Without the <code>g</code>
flag, the behavior is the same as <code>regexp_match</code>.#### `regexp_split_to_array(text: str, pattern: str [, flags: str]]) -> str[]`

Splits <code>text</code> by the regular expression <code>pattern</code> into an array.
If <code>flags</code> is set to <code>i</code>, matches case-insensitively.#### `repeat(s: str, n: int) -> str`

Replicate the string <code>n</code> times. The maximum length of the result string is 100 MiB.#### `replace(s: str, f: str, r: str) -> str`

<code>s</code> with all instances of <code>f</code> replaced with <code>r</code>. The maximum length of the result string is 100 MiB.#### `right(s: str, n: int) -> str`

The last <code>n</code> characters of <code>s</code>. If <code>n</code> is negative, all but the first <code>|n|</code> characters of <code>s</code>.#### `rtrim(s: str) -> str`

Trim all spaces from the right side of <code>s</code>.#### `rtrim(s: str, c: str) -> str`

Trim any character in <code>c</code> from the right side of <code>s</code>.#### `split_part(s: str, d: s, i: int) -> str`

Split <code>s</code> on delimiter <code>d</code>. Return the <code>str</code> at index <code>i</code>, counting from 1.#### `starts_with(s: str, prefix: str) -> bool`

Report whether <code>s</code> starts with <code>prefix</code>.#### `substring(s: str, start_pos: int) -> str`

Substring of <code>s</code> starting at <code>start_pos</code> [(docs)](/sql/functions/substring)#### `substring(s: str, start_pos: int, l: int) -> str`

Substring starting at <code>start_pos</code> of length <code>l</code> [(docs)](/sql/functions/substring)#### `substring('s' [FROM 'start_pos']? [FOR 'l']?) -> str`

Substring starting at <code>start_pos</code> of length <code>l</code> [(docs)](/sql/functions/substring)#### `translate(s: str, from: str, to: str) -> str`

Any character in <code>s</code> that matches a character in <code>from</code> is replaced by the corresponding character in <code>to</code>.
If <code>from</code> is longer than <code>to</code>, occurrences of the extra characters in <code>from</code> are removed.#### `trim([BOTH | LEADING | TRAILING]? ['c'? FROM]? 's') -> str`

<p>Trims any character in <code>c</code> from <code>s</code> on the specified side.</p>
<p>Defaults:</p>
<p>• Side: <code>BOTH</code></p>
<p>• <code>'c'</code>: <code>' '</code> (space)</p>
#### `try_parse_monotonic_iso8601_timestamp(s: str) -> timestamp`

Parses a specific subset of ISO8601 timestamps, returning <code>NULL</code> instead of
error on failure: <code>YYYY-MM-DDThh:mm:ss.sssZ</code> [(docs)](/sql/functions/pushdown)#### `upper(s: str) -> str`

Convert <code>s</code> to uppercase.#### `reverse(s: str) -> str`

Reverse the characters in <code>s</code>.#### `string_to_array(s: str, delimiter: str [, null_string: str]) -> str[]`

<p>Splits the string at occurrences of delimiter and returns a text array of
the split segments.</p>
<p>If <code>delimiter</code> is NULL, each character in the string will become a
separate element in the array.</p>
<p>If <code>delimiter</code> is an empty string, then the string is treated as a single
field.</p>
<p>If <code>null_string</code> is supplied and is not NULL, fields matching that string
are replaced by NULL.</p>
<p>For example: <code>string_to_array('xx~~yy~~zz', '~~', 'yy')</code> → <code>{xx,NULL,zz}</code></p>
### Scalar functionsScalar functions take a list of scalar expressions#### `expression bool_op ALL(s: Scalars) -> bool`

<code>true</code> if applying <a href="#boolean-operators" >bool_op</a> to <code>expression</code> and every value of <code>s</code> evaluates to <code>true</code>.#### `expression bool_op ANY(s: Scalars) -> bool`

<code>true</code> if applying <a href="#boolean-operators" >bool_op</a> to <code>expression</code> and any
value of <code>s</code> evaluates to <code>true</code>. Avoid using in equi-join conditions as
its use in the equi-join condition can lead to a significant increase in
memory usage. See <a href="/transform-data/idiomatic-materialize-sql/any" >idiomatic Materialize
SQL</a> for the alternative.#### `expression IN(s: Scalars) -> bool`

<code>true</code> for each value in <code>expression</code> if it matches at least one element of <code>s</code>.#### `expression NOT IN(s: Scalars) -> bool`

<code>true</code> for each value in <code>expression</code> if it does not match any elements of <code>s</code>.#### `expression bool_op SOME(s: Scalars) -> bool`

<code>true</code> if applying <a href="#boolean-operators" >bool_op</a> to <code>expression</code> and any value of <code>s</code> evaluates to <code>true</code>.### Subquery functionsSubquery functions take a query, e.g. <a href="/sql/select" ><code>SELECT</code></a>#### `expression bool_op ALL(s: Query) -> bool`

<code>s</code> must return exactly one column; <code>true</code> if applying <a href="#boolean-operators" >bool_op</a> to <code>expression</code> and every value of <code>s</code> evaluates to <code>true</code>.#### `expression bool_op ANY(s: Query) -> bool`

<code>s</code> must return exactly one column; <code>true</code> if applying <a href="#boolean-operators" >bool_op</a> to <code>expression</code> and any value of <code>s</code> evaluates to <code>true</code>.#### `csv_extract(num_csv_col: int, col_name: string) -> col1: string, ... coln: string`

Extracts separated values from a column containing a CSV file formatted as a string [(docs)](/sql/functions/csv_extract)#### `EXISTS(s: Query) -> bool`

<code>true</code> if <code>s</code> returns at least one row.#### `expression IN(s: Query) -> bool`

<code>s</code> must return exactly one column; <code>true</code> for each value in <code>expression</code> if it matches at least one element of <code>s</code>.#### `NOT EXISTS(s: Query) -> bool`

<code>true</code> if <code>s</code> returns zero rows.#### `expression NOT IN(s: Query) -> bool`

<code>s</code> must return exactly one column; <code>true</code> for each value in <code>expression</code> if it does not match any elements of <code>s</code>.#### `expression bool_op SOME(s: Query) -> bool`

<code>s</code> must return exactly one column; <code>true</code> if applying <a href="#boolean-operators" >bool_op</a> to <code>expression</code> and any value of <code>s</code> evaluates to <code>true</code>.### Date and time functionsTime functions take or produce a time-like type, e.g. <a href="../types/date" ><code>date</code></a>, <a href="../types/timestamp" ><code>timestamp</code></a>, <a href="../types/timestamptz" ><code>timestamp with time zone</code></a>.#### `age(timestamp, timestamp) -> interval`

Subtracts one timestamp from another, producing a &ldquo;symbolic&rdquo; result that uses years and months, rather than just days.#### `current_timestamp() -> timestamptz`

The <code>timestamp with time zone</code> representing when the query was executed.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `date_bin(stride: interval, source: timestamp, origin: timestamp) -> timestamp`

Align <code>source</code> with <code>origin</code> along <code>stride</code> [(docs)](/sql/functions/date-bin)#### `date_trunc(time_component: str, val: timestamp) -> timestamp`

Largest <code>time_component</code> &lt;= <code>val</code> [(docs)](/sql/functions/date-trunc)#### `date_trunc(time_component: str, val: interval) -> interval`

Largest <code>time_component</code> &lt;= <code>val</code> [(docs)](/sql/functions/date-trunc)#### `EXTRACT(extract_expr) -> numeric`

Specified time component from value [(docs)](/sql/functions/extract)#### `date_part(time_component: str, val: timestamp) -> float`

Specified time component from value [(docs)](/sql/functions/date-part)#### `mz_now() -> mz_timestamp`

The logical time at which a query executes. Used for temporal filters and query timestamp introspection [(docs)](/sql/functions/now_and_mz_now)

**Note:** This function is [unmaterializable](#unmaterializable-functions), but can be used in limited contexts in materialized views as a [temporal filter](/transform-data/patterns/temporal-filters/).#### `now() -> timestamptz`

The <code>timestamp with time zone</code> representing when the query was executed [(docs)](/sql/functions/now_and_mz_now)

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `timestamp AT TIME ZONE zone -> timestamptz`

Converts <code>timestamp</code> to the specified time zone, expressed as an offset from UTC [(docs)](/sql/functions/timezone-and-at-time-zone)

**Known limitation:** You must explicitly cast the type for the time zone.#### `timestamptz AT TIME ZONE zone -> timestamp`

Converts <code>timestamp with time zone</code> from UTC to the specified time zone, expressed as the local time [(docs)](/sql/functions/timezone-and-at-time-zone)

**Known limitation:** You must explicitly cast the type for the time zone.#### `timezone(zone, timestamp) -> timestamptz`

Converts <code>timestamp</code> to specified time zone, expressed as an offset from UTC [(docs)](/sql/functions/timezone-and-at-time-zone)

**Known limitation:** You must explicitly cast the type for the time zone.#### `timezone(zone, timestamptz) -> timestamp`

Converts <code>timestamp with time zone</code> from UTC to specified time zone, expressed as the local time [(docs)](/sql/functions/timezone-and-at-time-zone)

**Known limitation:** You must explicitly cast the type for the time zone.#### `timezone_offset(zone: str, when: timestamptz) ->
(abbrev: str, base_utc_offset: interval, dst_offset: interval)`

<p>Describes a time zone&rsquo;s offset from UTC at a specified moment.</p>
<p><code>zone</code> must be a valid IANA Time Zone Database identifier.</p>
<p><code>when</code> is a <code>timestamp with time zone</code> that specifies the moment at which to determine <code>zone</code>&rsquo;s offset from UTC.</p>
<p><code>abbrev</code> is the abbreviation for <code>zone</code> that is in use at the specified moment (e.g., <code>EST</code> or <code>EDT</code>).</p>
<p><code>base_utc_offset</code> is the base offset from UTC at the specified moment (e.g., <code>-5 hours</code>). Positive offsets mean east of Greenwich; negative offsets mean west of Greenwich.</p>
<p><code>dst_offset</code> is the additional offset at the specified moment due to Daylight Saving Time rules (e.g., <code>1 hours</code>). If non-zero, Daylight Saving Time is in effect.</p>
#### `to_timestamp(val: double precision) -> timestamptz`

Converts Unix epoch (seconds since 00:00:00 UTC on January 1, 1970) to timestamp.#### `to_char(val: timestamp, format: str)`

Converts a timestamp into a string using the specified format [(docs)](/sql/functions/to_char)#### `justify_days(val: interval) -> interval`

Adjust interval so 30-day time periods are represented as months [(docs)](/sql/functions/justify-days)#### `justify_hours(val: interval) -> interval`

Adjust interval so 24-hour time periods are represented as days [(docs)](/sql/functions/justify-hours)#### `justify_interval(val: interval) -> interval`

Adjust interval using justify_days and justify_hours, with additional sign adjustments [(docs)](/sql/functions/justify-interval)### UUID functions#### `uuid_generate_v5(namespace: uuid, name: text) -> uuid`

Generates a <a href="https://www.rfc-editor.org/rfc/rfc4122#page-7" >version 5 UUID</a> (SHA-1) in the given namespace using the specified input name.### JSON functions#### `jsonb_agg(expression) -> jsonb`

Aggregate values (including nulls) as a jsonb array [(docs)](/sql/functions/jsonb_agg)#### `jsonb_array_elements(j: jsonb) -> Col<jsonb>`

<code>j</code>&rsquo;s elements if <code>j</code> is an array [(docs)](/sql/types/jsonb#jsonb_array_elements)#### `jsonb_array_elements_text(j: jsonb) -> Col<string>`

<code>j</code>&rsquo;s elements if <code>j</code> is an array [(docs)](/sql/types/jsonb#jsonb_array_elements_text)#### `jsonb_array_length(j: jsonb) -> int`

Number of elements in <code>j</code>&rsquo;s outermost array [(docs)](/sql/types/jsonb#jsonb_array_length)#### `jsonb_build_array(x: ...) -> jsonb`

Output each element of <code>x</code> as a <code>jsonb</code> array. Elements can be of heterogenous types [(docs)](/sql/types/jsonb#jsonb_build_array)#### `jsonb_build_object(x: ...) -> jsonb`

The elements of x as a <code>jsonb</code> object. The argument list alternates between keys and values [(docs)](/sql/types/jsonb#jsonb_build_object)#### `jsonb_each(j: jsonb) -> Col<(key: string, value: jsonb)>`

<code>j</code>&rsquo;s outermost elements if <code>j</code> is an object [(docs)](/sql/types/jsonb#jsonb_each)#### `jsonb_each_text(j: jsonb) -> Col<(key: string, value: string)>`

<code>j</code>&rsquo;s outermost elements if <code>j</code> is an object [(docs)](/sql/types/jsonb#jsonb_each_text)#### `jsonb_object_agg(keys, values) -> jsonb`

Aggregate keys and values (including nulls) as a <code>jsonb</code> object [(docs)](/sql/functions/jsonb_object_agg)#### `jsonb_object_keys(j: jsonb) -> Col<string>`

<code>j</code>&rsquo;s outermost keys if <code>j</code> is an object [(docs)](/sql/types/jsonb#jsonb_object_keys)#### `jsonb_pretty(j: jsonb) -> string`

Pretty printed (i.e. indented) <code>j</code> [(docs)](/sql/types/jsonb#jsonb_pretty)#### `jsonb_typeof(j: jsonb) -> string`

Type of <code>j</code>&rsquo;s outermost value. One of <code>object</code>, <code>array</code>, <code>string</code>, <code>number</code>, <code>boolean</code>, and <code>null</code> [(docs)](/sql/types/jsonb#jsonb_typeof)#### `jsonb_strip_nulls(j: jsonb) -> jsonb`

<code>j</code> with all object fields with a value of <code>null</code> removed. Other <code>null</code> values remain [(docs)](/sql/types/jsonb#jsonb_strip_nulls)#### `to_jsonb(v: T) -> jsonb`

<code>v</code> as <code>jsonb</code> [(docs)](/sql/types/jsonb#to_jsonb)### Table functionsTable functions evaluate to a collection of rows rather than a single row. You can use the <code>WITH ORDINALITY</code> and
<code>ROWS FROM</code> clauses together with table functions. For more details, see <a href="/sql/functions/table-functions" >Table functions</a>.#### `generate_series(start: int, stop: int) -> Col<int>`

Generate all integer values between <code>start</code> and <code>stop</code>, inclusive.#### `generate_series(start: int, stop: int, step: int) -> Col<int>`

Generate all integer values between <code>start</code> and <code>stop</code>, inclusive, incrementing by <code>step</code> each time.#### `generate_series(start: timestamp, stop: timestamp, step: interval) -> Col<timestamp>`

Generate all timestamp values between <code>start</code> and <code>stop</code>, inclusive, incrementing by <code>step</code> each time.#### `generate_subscripts(a: anyarray, dim: int) -> Col<int>`

Generates a series comprising the valid subscripts of the <code>dim</code>&lsquo;th dimension of the given array <code>a</code>.#### `regexp_extract(regex: str, haystack: str) -> Col<string>`

Values of the capture groups of <code>regex</code> as matched in <code>haystack</code>. Outputs each capture group in a separate column. At least one capture group is needed. (The capture groups are the parts of the regular expression between parentheses.)#### `regexp_split_to_table(text: str, pattern: str [, flags: str]]) -> Col<string>`

Splits <code>text</code> by the regular expression <code>pattern</code>.
If <code>flags</code> is set to <code>i</code>, matches case-insensitively.#### `unnest(a: anyarray)`

Expands the array <code>a</code> into a set of rows.#### `unnest(l: anylist)`

Expands the list <code>l</code> into a set of rows.#### `unnest(m: anymap)`

Expands the map <code>m</code> in a set of rows with the columns <code>key</code> and <code>value</code>.### Array functions#### `array_cat(a1: arrayany, a2: arrayany) -> arrayany`

Concatenates <code>a1</code> and <code>a2</code>.#### `array_fill(anyelement, int[], [, int[]]) -> anyarray`

Returns an array initialized with supplied value and dimensions, optionally with lower bounds other than 1.#### `array_length(a: arrayany, dim: bigint) -> int`

Returns the length of the specified dimension of the array.#### `array_position(haystack: anycompatiblearray, needle: anycompatible) -> int`

Returns the subscript of <code>needle</code> in <code>haystack</code>. Returns <code>null</code> if not found.#### `array_position(haystack: anycompatiblearray, needle: anycompatible, skip: int) -> int`

Returns the subscript of <code>needle</code> in <code>haystack</code>, skipping the first <code>skip</code> elements. Returns <code>null</code> if not found.#### `array_to_string(a: anyarray, sep: text [, ifnull: text]) -> text`

> **Warning:** This function has the potential to produce very large strings and
> may cause queries to run out of memory or crash. Use with caution.

<p>Concatenates the elements of <code>array</code> together separated by <code>sep</code>.
Null elements are omitted unless <code>ifnull</code> is non-null, in which case
null elements are replaced with the value of <code>ifnull</code>.</p>#### `array_remove(a: anyarray, e: anyelement) -> anyarray`

Returns the array <code>a</code> without any elements equal to the given value <code>e</code>.
The array must be one-dimensional. Comparisons are done using `IS NOT
DISTINCT FROM semantics, so it is possible to remove NULLs.### Hash functions#### `crc32(data: bytea) -> uint32`

Computes the 32-bit cyclic redundancy check of the given bytea <code>data</code> using the IEEE 802.3 polynomial.#### `crc32(data: text) -> uint32`

Computes the 32-bit cyclic redundancy check of the given text <code>data</code> using the IEEE 802.3 polynomial.#### `digest(data: text, type: text) -> bytea`

Computes a binary hash of the given text <code>data</code> using the specified <code>type</code> algorithm.
Supported hash algorithms are: <code>md5</code>, <code>sha1</code>, <code>sha224</code>, <code>sha256</code>, <code>sha384</code>, and <code>sha512</code>.#### `digest(data: bytea, type: text) -> bytea`

Computes a binary hash of the given bytea <code>data</code> using the specified <code>type</code> algorithm.
The supported hash algorithms are the same as for the text variant of this function.#### `hmac(data: text, key: text, type: text) -> bytea`

Computes a hashed MAC of the given text <code>data</code> using the specified <code>key</code> and
<code>type</code> algorithm. Supported hash algorithms are the same as for <code>digest</code>.#### `hmac(data: bytea, key: bytea, type: text) -> bytea`

Computes a hashed MAC of the given bytea <code>data</code> using the specified <code>key</code> and
<code>type</code> algorithm. The supported hash algorithms are the same as for <code>digest</code>.#### `kafka_murmur2(data: bytea) -> integer`

Computes the Murmur2 hash of the given bytea <code>data</code> using the seed used by Kafka&rsquo;s default partitioner and with the high bit cleared.#### `kafka_murmur2(data: text) -> integer`

Computes the Murmur2 hash of the given text <code>data</code> using the seed used by Kafka&rsquo;s default partitioner and with the high bit cleared.#### `md5(data: bytea) -> text`

Computes the MD5 hash of the given bytea <code>data</code>.
For PostgreSQL compatibility, returns a hex-encoded value of type <code>text</code> rather than <code>bytea</code>.#### `seahash(data: bytea) -> u64`

Computes the <a href="https://docs.rs/seahash" >SeaHash</a> hash of the given bytea <code>data</code>.#### `seahash(data: text) -> u64`

Computes the <a href="https://docs.rs/seahash" >SeaHash</a> hash of the given text <code>data</code>.#### `sha224(data: bytea) -> bytea`

Computes the SHA-224 hash of the given bytea <code>data</code>.#### `sha256(data: bytea) -> bytea`

Computes the SHA-256 hash of the given bytea <code>data</code>.#### `sha384(data: bytea) -> bytea`

Computes the SHA-384 hash of the given bytea <code>data</code>.#### `sha512(data: bytea) -> bytea`

Computes the SHA-512 hash of the given bytea <code>data</code>.### Window functions> **Tip:** For some window function query patterns, rewriting your query to not use
> window functions can yield better performance.  See [Idiomatic Materialize SQL](/transform-data/idiomatic-materialize-sql/) for details.

<p>Window functions compute values across sets of rows related to the current row.
For example, you can use a window aggregation to smooth measurement data by computing the average of the last 5
measurements before every row as follows:</p>
<pre tabindex="0"><code>SELECT
  avg(measurement) OVER (ORDER BY time ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
FROM measurements;
</code></pre><p>Window functions always need an <code>OVER</code> clause. For the <code>OVER</code> clause, Materialize supports the same
<a href="https://www.postgresql.org/docs/current/tutorial-window.html" >syntax as
PostgreSQL</a>,
but supports only the following frame modes:</p>
<ul>
<li>
<p>the <code>ROWS</code> frame mode.</p>
</li>
<li>
<p>the default frame, which is <code>RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW</code>.</p>
</li>
</ul>
> **Note:** For [window functions](/sql/functions/#window-functions), when an input record
> in a partition (as determined by the `PARTITION BY` clause of your window
> function) is added/removed/changed, Materialize recomputes the results for the
> entire window partition. This means that when a new batch of input data arrives
> (that is, every second), **the amount of computation performed is proportional
> to the total size of the touched partitions**.
> For example, assume that in a given second, 20 input records change, and these
> records belong to **10** different partitions, where the average size of each
> partition is **100**. Then, amount of work to perform is proportional to
> computing the window function results for **10\*100=1000** rows.
> To avoid performance issues that may arise as the number of records grows,
> consider rewriting your query to use idiomatic Materialize SQL instead of window
> functions. If your query cannot be rewritten without the window functions and
> the performance of window functions is insufficient for your use case, please
> [contact our team](/support/).
> See [Idiomatic Materialize SQL](/transform-data/idiomatic-materialize-sql/)
> for examples of rewriting window functions.

<p>In addition to the below window functions, you can use the <code>OVER</code> clause with any <a href="#aggregate-functions" >aggregation function</a>
(e.g., <code>sum</code>, <code>avg</code>) as well. Using an aggregation with an <code>OVER</code> clause is called a <em>window aggregation</em>. A
window aggregation computes the aggregate not on the groups specified by the <code>GROUP BY</code> clause, but on the frames
specified inside the <code>OVER</code> clause. (Note that a window aggregation produces exactly one output value <em>for each input
row</em>. This is different from a standard aggregation, which produces one output value for each <em>group</em> specified by
the <code>GROUP BY</code> clause.)</p>
#### `dense_rank() -> int`

Returns the rank of the current row within its partition without gaps, counting from 1.
Rows that compare equal will have the same rank.#### `first_value(value anycompatible) -> anyelement`

<p>Returns <code>value</code> evaluated at the first row of the window frame. The default window frame is
<code>RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW</code>.</p>
<p>See also <a href="/transform-data/idiomatic-materialize-sql/first-value/" >Idiomatic Materialize SQL: First value</a>.</p>
#### `lag(value anycompatible [, offset integer [, default anycompatible ]]) -> int`

<p>Returns <code>value</code> evaluated at the row that is <code>offset</code> rows before the current row within the partition;
if there is no such row, instead returns <code>default</code> (which must be of a type compatible with <code>value</code>).
If <code>offset</code> is <code>NULL</code>, <code>NULL</code> is returned instead.
Both <code>offset</code> and <code>default</code> are evaluated with respect to the current row.
If omitted, <code>offset</code> defaults to 1 and <code>default</code> to <code>NULL</code>.</p>
<p>See also <a href="/transform-data/idiomatic-materialize-sql/lag/" >Idiomatic Materialize SQL: Lag over</a>.</p>
#### `last_value(value anycompatible) -> anyelement`

<p>Returns <code>value</code> evaluated at the last row of the window frame. The default window frame is
<code>RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW</code>.</p>
<p>See also <a href="/transform-data/idiomatic-materialize-sql/last-value/" >Idiomatic Materialize SQL: Last
value</a>.</p>
#### `lead(value anycompatible [, offset integer [, default anycompatible ]]) -> int`

<p>Returns <code>value</code> evaluated at the row that is <code>offset</code> rows after the current row within the partition;
if there is no such row, instead returns <code>default</code> (which must be of a type compatible with <code>value</code>).
If <code>offset</code> is <code>NULL</code>, <code>NULL</code> is returned instead.
Both <code>offset</code> and <code>default</code> are evaluated with respect to the current row.
If omitted, <code>offset</code> defaults to 1 and <code>default</code> to <code>NULL</code>.</p>
<p>See also <a href="/transform-data/idiomatic-materialize-sql/lead/" >Idiomatic Materialize SQL: Lead
over</a>.</p>
#### `rank() -> int`

Returns the rank of the current row within its partition with gaps (counting from 1):
rows that compare equal will have the same rank, and then the rank is incremented by the number of rows that
compared equal.#### `row_number() -> int`

<p>Returns the number of the current row within its partition, counting from 1.
Rows that compare equal will be ordered in an unspecified way.</p>
<p>See also <a href="/transform-data/idiomatic-materialize-sql/top-k/" >Idiomatic Materialize SQL: Top-K</a>.</p>
### System information functionsFunctions that return information about the system.#### `mz_environment_id() -> text`

Returns a string containing a <code>uuid</code> uniquely identifying the Materialize environment.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `mz_uptime() -> interval`

Returns the length of time that the materialized process has been running.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `mz_version() -> text`

Returns the server&rsquo;s version information as a human-readable string.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `mz_version_num() -> int`

Returns the server&rsquo;s version as an integer having the format <code>XXYYYZZ</code>, where <code>XX</code> is the major version, <code>YYY</code> is the minor version and <code>ZZ</code> is the patch version.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `current_database() -> text`

Returns the name of the current database.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `current_catalog() -> text`

Alias for <code>current_database</code>.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `current_user() -> text`

Returns the name of the user who executed the containing query.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `current_role() -> text`

Alias for <code>current_user</code>.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `user() -> text`

Alias for <code>current_user</code>.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `session_user() -> text`

Returns the name of the user who initiated the database connection.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `mz_row_size(expr: Record) -> int`

Returns the number of bytes used to store a row.### PostgreSQL compatibility functionsFunctions whose primary purpose is to facilitate compatibility with PostgreSQL tools.
These functions may have suboptimal performance characteristics.#### `format_type(oid: int, typemod: int) -> text`

Returns the canonical SQL name for the type specified by <code>oid</code> with <code>typemod</code> applied.#### `current_schema() -> text`

Returns the name of the first non-implicit schema on the search path, or
<code>NULL</code> if the search path is empty.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `current_schemas(include_implicit: bool) -> text[]`

Returns the names of the schemas on the search path.
The <code>include_implicit</code> parameter controls whether implicit schemas like
<code>mz_catalog</code> and <code>pg_catalog</code> are included in the output.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `current_setting(setting_name: text[, missing_ok: bool]) -> text`

Returns the value of the named setting or error if it does not exist.
If <code>missing_ok</code> is true, return NULL if it does not exist.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `obj_description(oid: oid, catalog: text) -> text`

Returns the comment for a database object specified by its <code>oid</code> and the name of the containing system catalog.#### `col_description(oid: oid, column: int) -> text`

Returns the comment for a table column, which is specified by the <code>oid</code> of its table and its column number.#### `pg_backend_pid() -> int`

Returns the internal connection ID.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `pg_cancel_backend(connection_id: int) -> bool`

Cancels an in-progress query on the specified connection ID.
Returns whether the connection ID existed (not if it cancelled a query).

**Note:** This function is [side-effecting](#side-effecting-functions).#### `pg_column_size(expr: any) -> int`

Returns the number of bytes used to store any individual data value.#### `pg_size_pretty(expr: numeric) -> text`

Converts a size in bytes into a human-readable format.#### `pg_get_constraintdef(oid: oid[, pretty: bool]) -> text`

Returns the constraint definition for the given <code>oid</code>. Currently always
returns NULL since constraints aren&rsquo;t supported.#### `pg_get_indexdef(index: oid[, column: integer, pretty: bool]) -> text`

Reconstructs the creating command for an index. (This is a decompiled
reconstruction, not the original text of the command.) If column is
supplied and is not zero, only the definition of that column is reconstructed.#### `pg_get_ruledef(rule_oid: oid[, pretty bool]) -> text`

Reconstructs the creating command for a rule. This function
always returns NULL because Materialize does not support rules.#### `pg_get_userbyid(role: oid) -> text`

Returns the role (user) name for the given <code>oid</code>. If no role matches the
specified OID, the string <code>unknown (OID=oid)</code> is returned.#### `pg_get_viewdef(view_name: text[, pretty: bool]) -> text`

Returns the underlying SELECT command for the given view.#### `pg_get_viewdef(view_oid: oid[, pretty: bool]) -> text`

Returns the underlying SELECT command for the given view.#### `pg_get_viewdef(view_oid: oid[, wrap_column: integer]) -> text`

Returns the underlying SELECT command for the given view.#### `pg_has_role([user: name or oid,] role: text or oid, privilege: text) -> bool`

Alias for <code>has_role</code> for PostgreSQL compatibility.#### `pg_is_in_recovery() -> bool`

Returns if the a recovery is still in progress.#### `pg_table_is_visible(relation: oid) -> boolean`

Reports whether the relation with the specified OID is visible in the search path.#### `pg_tablespace_location(tablespace: oid) -> text`

Returns the path in the file system that the provided tablespace is on.#### `pg_type_is_visible(relation: oid) -> boolean`

Reports whether the type with the specified OID is visible in the search path.#### `pg_function_is_visible(relation: oid) -> boolean`

Reports whether the function with the specified OID is visible in the search path.#### `pg_typeof(expr: any) -> text`

Returns the type of its input argument as a string.#### `pg_encoding_to_char(encoding_id: integer) -> text`

PostgreSQL compatibility shim. Not intended for direct use.#### `pg_postmaster_start_time() -> timestamptz`

Returns the time when the server started.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `pg_relation_size(relation: regclass[, fork: text]) -> bigint`

Disk space used by the specified fork (&lsquo;main&rsquo;, &lsquo;fsm&rsquo;, &lsquo;vm&rsquo;, or &lsquo;init&rsquo;)
of the specified table or index. If no fork is specified, it defaults
to &lsquo;main&rsquo;. This function always returns -1 because Materialize does
not store tables and indexes on local disk.#### `pg_stat_get_numscans(oid: oid) -> bigint`

Number of sequential scans done when argument is a table,
or number of index scans done when argument is an index.
This function always returns -1 because Materialize does
not collect statistics.#### `version() -> text`

Returns a PostgreSQL-compatible version string.

**Note:** This function is [unmaterializable](#unmaterializable-functions).### Access privilege inquiry functionsFunctions that allow querying object access privileges. None of the following functions consider
whether the provided role is a <em>superuser</em> or not.#### `has_cluster_privilege([role: text or oid,] cluster: text, privilege: text) -> bool`

Reports whether the role with the specified role name or OID has the privilege on
the cluster with the specified cluster name. If the role is omitted then
the <code>current_role</code> is assumed.#### `has_connection_privilege([role: text or oid,] connection: text or oid, privilege: text) -> bool`

Reports whether the role with the specified role name or OID has the privilege on
the connection with the specified connection name or OID. If the role is omitted then
the <code>current_role</code> is assumed.#### `has_database_privilege([role: text or oid,] database: text or oid, privilege: text) -> bool`

Reports whether the role with the specified role name or OID has the privilege on
the database with the specified database name or OID. If the role is omitted then
the <code>current_role</code> is assumed.#### `has_schema_privilege([role: text or oid,] schema: text or oid, privilege: text) -> bool`

Reports whether the role with the specified role name or OID has the privilege on
the schema with the specified schema name or OID. If the role is omitted then
the <code>current_role</code> is assumed.#### `has_role([user: name or oid,] role: text or oid, privilege: text) -> bool`

Reports whether the <code>user</code> has the privilege for <code>role</code>. <code>privilege</code> can either be <code>MEMBER</code>
or <code>USAGE</code>, however currently this value is ignored. The <code>PUBLIC</code> pseudo-role cannot be used
for the <code>user</code> nor the <code>role</code>. If the <code>user</code> is omitted then the <code>current_role</code> is assumed.#### `has_secret_privilege([role: text or oid,] secret: text or oid, privilege: text) -> bool`

Reports whether the role with the specified role name or OID has the privilege on
the secret with the specified secret name or OID. If the role is omitted then
the <code>current_role</code> is assumed.#### `has_system_privilege([role: text or oid,] privilege: text) -> bool`

Reports whether the role with the specified role name or OID has the system privilege.
If the role is omitted then the <code>current_role</code> is assumed.#### `has_table_privilege([role: text or oid,] relation: text or oid, privilege: text) -> bool`

Reports whether the role with the specified role name or OID has the privilege on
the relation with the specified relation name or OID. If the role is omitted then
the <code>current_role</code> is assumed.#### `has_type_privilege([role: text or oid,] type: text or oid, privilege: text) -> bool`

Reports whether the role with the specified role name or OID has the privilege on
the type with the specified type name or OID. If the role is omitted then
the <code>current_role</code> is assumed.#### `mz_is_superuser() -> bool`

Reports whether the <code>current_role</code> is a superuser.

**Note:** This function is [unmaterializable](#unmaterializable-functions).

## Operators

### Generic operators

Operator | Computes
---------|---------
`val::type` | Cast of `val` as `type` ([docs](cast))

### Boolean operators

Operator | Computes
---------|---------
`AND` | Boolean "and"
`OR` | Boolean "or"
`=` | Equality. Do not use with `NULL` as `= NULL` always evaluates to `NULL`; instead, use `IS NULL` for null checks.
`<>` | Inequality. Do not use with `NULL` as `<> NULL` always evaluates to `NULL`; instead, use `IS NOT NULL` for null checks.
`!=` | Inequality. Do not use with `NULL` as `!= NULL` always evaluates to `NULL`; instead, use `IS NOT NULL` for null checks.
`<` | Less than
`>` | Greater than
`<=` | Less than or equal to
`>=` | Greater than or equal to
`a BETWEEN x AND y` | `a >= x AND a <= y`
`a NOT BETWEEN x AND y` | `a < x OR a > y`
`a IS NULL` | Evaluates to true if the value of a is `NULL`.
`a ISNULL` | Evaluates to true if the value of a is `NULL`.
`a IS NOT NULL` | Evaluates to true if the value of a is **NOT** `NULL`.
`a IS TRUE` | `a` is true, requiring `a` to be a boolean
`a IS NOT TRUE` | `a` is not true, requiring `a` to be a boolean
`a IS FALSE` | `a` is false, requiring `a` to be a boolean
`a IS NOT FALSE` | `a` is not false, requiring `a` to be a boolean
`a IS UNKNOWN` | `a = NULL`, requiring `a` to be a boolean
`a IS NOT UNKNOWN` | `a != NULL`, requiring `a` to be a boolean
`a LIKE match_expr [ ESCAPE escape_char ]` | `a` matches `match_expr`, using [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE)
`a ILIKE match_expr [ ESCAPE escape_char ]` | `a` matches `match_expr`, using case-insensitive [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE)

### Numbers operators

Operator | Computes
---------|---------
`+` | Addition
`-` | Subtraction
`*` | Multiplication
`/` | Division
`%` | Modulo
`&` | Bitwise AND
<code>&vert;</code> | Bitwise OR
`#` | Bitwise XOR
`~` | Bitwise NOT
`<<`| Bitwise left shift
`>>`| Bitwise right shift

### String operators

Operator | Computes
---------|---------
<code>&vert;&vert;</code> | Concatenation
`~~` | Matches LIKE pattern case sensitively, see [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE)
`~~*` | Matches LIKE pattern case insensitively (ILIKE), see [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE)
`!~~` | Matches NOT LIKE pattern (case sensitive), see [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE)
`!~~*` | Matches NOT ILIKE pattern (case insensitive), see [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE)
`~` | Matches regular expression, case sensitive
`~*` | Matches regular expression, case insensitive
`!~` | Matches regular expression case sensitively, and inverts the match
`!~*` | Match regular expression case insensitively, and inverts the match

The regular expression syntax supported by Materialize is documented by the
[Rust `regex` crate](https://docs.rs/regex/*/#syntax).
The maximum length of a regular expression is 1 MiB in its raw form, and 10 MiB
after compiling it.

> **Warning:** Materialize regular expressions are similar to, but not identical to, PostgreSQL
> regular expressions.


### Time-like operators

Operation | Computes
----------|------------
[`date`](../types/date) `+` [`interval`](../types/interval) | [`timestamp`](../types/timestamp)
[`date`](../types/date) `-` [`interval`](../types/interval) | [`timestamp`](../types/timestamp)
[`date`](../types/date) `+` [`time`](../types/time) | [`timestamp`](../types/timestamp)
[`date`](../types/date) `-` [`date`](../types/date) | [`integer`](../types/integer)
[`timestamp`](../types/timestamp) `+` [`interval`](../types/interval) | [`timestamp`](../types/timestamp)
[`timestamp`](../types/timestamp) `-` [`interval`](../types/interval) | [`timestamp`](../types/timestamp)
[`timestamp`](../types/timestamp) `-` [`timestamp`](../types/timestamp) | [`interval`](../types/interval)
[`time`](../types/time) `+` [`interval`](../types/interval) | `time`
[`time`](../types/time) `-` [`interval`](../types/interval) | `time`
[`time`](../types/time) `-` [`time`](../types/time) | [`interval`](../types/interval)

### JSON operators

Operator | RHS Type | Description
---------|----------|-------------
`->` | `text`, `int`| Access field by name or index position, and return `jsonb` ([docs](/sql/types/jsonb/#field-access-as-jsonb--))
`->>` | `text`, `int`| Access field by name or index position, and return `text` ([docs](/sql/types/jsonb/#field-access-as-text--))
`#>` | `text[]` | Access field by path, and return `jsonb` ([docs](/sql/types/jsonb/#path-access-as-jsonb-))
`#>>` | `text[]` | Access field by path, and return `text` ([docs](/sql/types/jsonb/#path-access-as-text-))
<code>&vert;&vert;</code> | `jsonb` | Concatenate LHS and RHS ([docs](/sql/types/jsonb/#jsonb-concat-))
`-` | `text` | Delete all values with key of RHS ([docs](/sql/types/jsonb/#remove-key--))
`@>` | `jsonb` | Does element contain RHS? ([docs](/sql/types/jsonb/#lhs-contains-rhs-))
<code>&lt;@</code> | `jsonb` | Does RHS contain element? ([docs](/sql/types/jsonb/#rhs-contains-lhs-))
`?` | `text` | Is RHS a top-level key? ([docs](/sql/types/jsonb/#search-top-level-keys-))


### Map operators

Operator | RHS Type | Description
---------|----------|-------------
`->` | `string` | Access field by name, and return target field ([docs](/sql/types/map/#retrieve-value-with-key--))
`@>` | `map` | Does element contain RHS? ([docs](/sql/types/map/#lhs-contains-rhs-))
<code>&lt;@</code> | `map` | Does RHS contain element? ([docs](/sql/types/map/#rhs-contains-lhs-))
`?` | `string` | Is RHS a top-level key? ([docs](/sql/types/map/#search-top-level-keys-))
`?&` | `string[]` | Does LHS contain all RHS top-level keys? ([docs](/sql/types/map/#search-for-all-top-level-keys-))
<code>?&#124;</code> | `string[]` | Does LHS contain any RHS top-level keys? ([docs](/sql/types/map/#search-for-any-top-level-keys-))


### List operators

List operators are [polymorphic](../types/list/#polymorphism).

Operator | Description
---------|-------------
<code>listany &vert;&vert; listany</code> | Concatenate the two lists.
<code>listany &vert;&vert; listelementany</code> | Append the element to the list.
<code>listelementany &vert;&vert; listany</code> | Prepend the element to the list.
<code>listany @&gt; listany</code> | Check if the first list contains all elements of the second list.
<code>listany &lt;@ listany</code> | Check if all elements of the first list are contained in the second list.




---

## Aggregate function filters


You can use a `FILTER` clause on an aggregate function to specify which rows are sent to an [aggregate function](/sql/functions/#aggregate-functions). Rows for which the `filter_clause` evaluates to true contribute to the aggregation.

Temporal filters cannot be used in aggregate function filters.

## Syntax



```mzsql
<aggregate_name> ( <expression> )
FILTER (WHERE <filter_clause>)

```

| Syntax element | Description |
| --- | --- |
| `<aggregate_name>` | The name of the aggregate function.  |
| `<expression>` | The expression to aggregate.  |
| **FILTER** (WHERE `<filter_clause>`) | Specifies which rows are sent to the aggregate function. Rows for which the `<filter_clause>` evaluates to true contribute to the aggregation. Temporal filters cannot be used in aggregate function filters.  |


## Examples

```mzsql
SELECT
    COUNT(*) AS unfiltered,
    -- The FILTER guards the evaluation which might otherwise error.
    COUNT(1 / (5 - i)) FILTER (WHERE i < 5) AS filtered
FROM generate_series(1,10) AS s(i)
```


---

## array_agg function


The `array_agg(value)` function aggregates values (including nulls) as an array.
The input values to the aggregate can be [filtered](../filters).

## Syntax



```mzsql
array_agg ( <value>
  [ORDER BY <col_ref> [ASC | DESC] [NULLS FIRST | NULLS LAST] [, ...]]
)
[FILTER (WHERE <filter_clause>)]

```

| Syntax element | Description |
| --- | --- |
| `<value>` | The values you want aggregated.  |
| **ORDER BY** `<col_ref>` [**ASC** \| **DESC**] [**NULLS FIRST** \| **NULLS LAST**] [, ...] | Optional. Specifies the ordering of values within the aggregation. If not specified, incoming rows are not guaranteed any order.  |
| **FILTER** (WHERE `<filter_clause>`) | Optional. Specifies which rows are sent to the aggregate function. Rows for which the `<filter_clause>` evaluates to true contribute to the aggregation. See [Aggregate function filters](/sql/functions/filters) for details.  |


## Signatures

Parameter | Type | Description
----------|------|------------
_value_ | [any](../../types) | The values you want aggregated.

### Return value

`array_agg` returns the aggregated values as an [array](../../types/array/).

This function always executes on the data from `value` as if it were sorted in ascending order before the function call. Any specified ordering is
ignored. If you need to perform aggregation in a specific order, you must specify `ORDER BY` within the aggregate function call itself. Otherwise incoming rows are not guaranteed any order.

## Details

### Usage in dataflows

While `array_agg` is available in Materialize, materializing `array_agg(values)`
is considered an incremental view maintenance anti-pattern. Any change to the data
underlying the function call will require the function to be recomputed entirely,
discarding the benefits of maintaining incremental updates.

Instead, we recommend that you materialize all components required for the
`array_agg` function call and create a non-materialized view using `array_agg`
on top of that. That pattern is illustrated in the following statements:

```mzsql
CREATE MATERIALIZED VIEW foo_view AS SELECT * FROM foo;
CREATE VIEW bar AS SELECT array_agg(foo_view.bar) FROM foo_view;
```

## Examples

```mzsql
SELECT
    title,
    ARRAY_AGG (
        first_name || ' ' || last_name
        ORDER BY
            last_name
    ) actors
FROM
    film
GROUP BY
    title;
```


---

## CAST function and operator


The `cast` function and operator return a value converted to the specified [type](../../types/).

## Signatures



```mzsql
CAST ( <val> AS <type> )

```

| Syntax element | Description |
| --- | --- |
| `<val>` | Any value you want to convert.  |
| **AS** `<type>` | The return value's [type](/sql/types/).  |




```mzsql
<val>::<type>

```

| Syntax element | Description |
| --- | --- |
| `<val>` | Any value you want to convert.  |
| `::<type>` | The return value's [type](/sql/types/).  |


The following special syntax is permitted if _val_ is a string literal:



```mzsql
<type> '<val>'

```

| Syntax element | Description |
| --- | --- |
| `<type>` | The return value's [type](/sql/types/).  |
| `'<val>'` | A string literal value. This special syntax is permitted if `<val>` is a string literal.  |


### Return value

`cast` returns the value with the type specified by the _type_ parameter.

## Details

### Valid casts

Cast context defines when casts may occur.

Cast context | Definition | Strictness
--------|------------|-----------
**Implicit** | Values are automatically converted. For example, when you add `int4` to `int8`, the `int4` value is automatically converted to `int8`. | Least
**Assignment** | Values of one type are converted automatically when inserted into a column of a different type. | Medium
**Explicit** | You must invoke `CAST` deliberately. | Most

Casts allowed in less strict contexts are also allowed in stricter contexts. That is, implicit casts also occur by assignment, and both implicit casts and casts by assignment can be explicitly invoked.

Source type                                | Return type                                   | Cast context
-------------------------------------------|-----------------------------------------------|----------
[`array`](../../types/array/)<sup>1</sup>  | [`text`](../../types/text/)                   | Assignment
[`bigint`](../../types/integer/)           | [`bool`](../../types/boolean/)                | Explicit
[`bigint`](../../types/integer/)           | [`int`](../../types/integer/)                 | Assignment
[`bigint`](../../types/integer/)           | [`float`](../../types/float/)                 | Implicit
[`bigint`](../../types/integer/)           | [`numeric`](../../types/numeric/)             | Implicit
[`bigint`](../../types/integer/)           | [`real`](../../types/real/)                   | Implicit
[`bigint`](../../types/integer/)           | [`text`](../../types/text/)                   | Assignment
[`bigint`](../../types/integer/)           | [`uint2`](../../types/uint/)                  | Assignment
[`bigint`](../../types/integer/)           | [`uint4`](../../types/uint/)                  | Assignment
[`bigint`](../../types/integer/)           | [`uint8`](../../types/uint/)                  | Assignment
[`bool`](../../types/boolean/)             | [`int`](../../types/integer/)                 | Explicit
[`bool`](../../types/boolean/)             | [`text`](../../types/text/)                   | Assignment
[`bytea`](../../types/bytea/)              | [`text`](../../types/text/)                   | Assignment
[`date`](../../types/date/)                | [`text`](../../types/text/)                   | Assignment
[`date`](../../types/date/)                | [`timestamp`](../../types/timestamp/)         | Implicit
[`date`](../../types/date/)                | [`timestamptz`](../../types/timestamp/)       | Implicit
[`float`](../../types/float/)              | [`bigint`](../../types/integer/)              | Assignment
[`float`](../../types/float/)              | [`int`](../../types/integer/)                 | Assignment
[`float`](../../types/float/)              | [`numeric`](../../types/numeric/)<sup>2</sup> | Assignment
[`float`](../../types/float/)              | [`real`](../../types/real/)                   | Assignment
[`float`](../../types/float/)              | [`text`](../../types/text/)                   | Assignment
[`float`](../../types/float/)              | [`uint2`](../../types/uint/)                  | Assignment
[`float`](../../types/float/)              | [`uint4`](../../types/uint/)                  | Assignment
[`float`](../../types/float/)              | [`uint8`](../../types/uint/)                  | Assignment
[`int`](../../types/integer/)              | [`bigint`](../../types/integer/)              | Implicit
[`int`](../../types/integer/)              | [`bool`](../../types/boolean/)                | Explicit
[`int`](../../types/integer/)              | [`float`](../../types/float/)                 | Implicit
[`int`](../../types/integer/)              | [`numeric`](../../types/numeric/)             | Implicit
[`int`](../../types/integer/)              | [`oid`](../../types/oid/)                     | Implicit
[`int`](../../types/integer/)              | [`real`](../../types/real/)                   | Implicit
[`int`](../../types/integer/)              | [`text`](../../types/text/)                   | Assignment
[`int`](../../types/integer/)              | [`uint2`](../../types/uint/)                  | Assignment
[`int`](../../types/integer/)              | [`uint4`](../../types/uint/)                  | Assignment
[`int`](../../types/integer/)              | [`uint8`](../../types/uint/)                  | Assignment
[`interval`](../../types/interval/)        | [`text`](../../types/text/)                   | Assignment
[`interval`](../../types/interval/)        | [`time`](../../types/time/)                   | Assignment
[`jsonb`](../../types/jsonb/)              | [`bigint`](../../types/integer/)              | Explicit
[`jsonb`](../../types/jsonb/)              | [`float`](../../types/float/)                 | Explicit
[`jsonb`](../../types/jsonb/)              | [`int`](../../types/integer/)                 | Explicit
[`jsonb`](../../types/jsonb/)              | [`real`](../../types/real/)                   | Explicit
[`jsonb`](../../types/jsonb/)              | [`numeric`](../../types/numeric/)             | Explicit
[`jsonb`](../../types/jsonb/)              | [`text`](../../types/text/)                   | Assignment
[`list`](../../types/list/)<sup>1</sup>    | [`list`](../../types/list/)                   | Implicit
[`list`](../../types/list/)<sup>1</sup>    | [`text`](../../types/text/)                   | Assignment
[`map`](../../types/map/)                  | [`text`](../../types/text/)                   | Assignment
[`mz_aclitem`](../../types/mz_aclitem/)    | [`text`](../../types/text/)                   | Explicit
[`numeric`](../../types/numeric/)          | [`bigint`](../../types/integer/)              | Assignment
[`numeric`](../../types/numeric/)          | [`float`](../../types/float/)                 | Implicit
[`numeric`](../../types/numeric/)          | [`int`](../../types/integer/)                 | Assignment
[`numeric`](../../types/numeric/)          | [`real`](../../types/real/)                   | Implicit
[`numeric`](../../types/numeric/)          | [`text`](../../types/text/)                   | Assignment
[`numeric`](../../types/numeric/)          | [`uint2`](../../types/uint/)                  | Assignment
[`numeric`](../../types/numeric/)          | [`uint4`](../../types/uint/)                  | Assignment
[`numeric`](../../types/numeric/)          | [`uint8`](../../types/uint/)                  | Assignment
[`oid`](../../types/oid/)                  | [`int`](../../types/integer/)                 | Assignment
[`oid`](../../types/oid/)                  | [`text`](../../types/text/)                   | Explicit
[`real`](../../types/real/)                | [`bigint`](../../types/integer/)              | Assignment
[`real`](../../types/real/)                | [`float`](../../types/float/)                 | Implicit
[`real`](../../types/real/)                | [`int`](../../types/integer/)                 | Assignment
[`real`](../../types/real/)                | [`numeric`](../../types/numeric/)             | Assignment
[`real`](../../types/real/)                | [`text`](../../types/text/)                   | Assignment
[`real`](../../types/real/)                | [`uint2`](../../types/uint/)                  | Assignment
[`real`](../../types/real/)                | [`uint4`](../../types/uint/)                  | Assignment
[`real`](../../types/real/)                | [`uint8`](../../types/uint/)                  | Assignment
[`record`](../../types/record/)            | [`text`](../../types/text/)                   | Assignment
[`smallint`](../../types/integer/)         | [`bigint`](../../types/integer/)              | Implicit
[`smallint`](../../types/integer/)         | [`float`](../../types/float/)                 | Implicit
[`smallint`](../../types/integer/)         | [`int`](../../types/integer/)                 | Implicit
[`smallint`](../../types/integer/)         | [`numeric`](../../types/numeric/)             | Implicit
[`smallint`](../../types/integer/)         | [`oid`](../../types/oid/)                     | Implicit
[`smallint`](../../types/integer/)         | [`real`](../../types/real/)                   | Implicit
[`smallint`](../../types/integer/)         | [`text`](../../types/text/)                   | Assignment
[`smallint`](../../types/integer/)         | [`uint2`](../../types/uint/)                  | Assignment
[`smallint`](../../types/integer/)         | [`uint4`](../../types/uint/)                  | Assignment
[`smallint`](../../types/integer/)         | [`uint8`](../../types/uint/)                  | Assignment
[`text`](../../types/text/)                | [`bigint`](../../types/integer/)              | Explicit
[`text`](../../types/text/)                | [`bool`](../../types/boolean/)                | Explicit
[`text`](../../types/text/)                | [`bytea`](../../types/bytea/)                 | Explicit
[`text`](../../types/text/)                | [`date`](../../types/date/)                   | Explicit
[`text`](../../types/text/)                | [`float`](../../types/float/)                 | Explicit
[`text`](../../types/text/)                | [`int`](../../types/integer/)                 | Explicit
[`text`](../../types/text/)                | [`interval`](../../types/interval/)           | Explicit
[`text`](../../types/text/)                | [`jsonb`](../../types/jsonb/)                 | Explicit
[`text`](../../types/text/)                | [`list`](../../types/list/)                   | Explicit
[`text`](../../types/text/)                | [`map`](../../types/map/)                     | Explicit
[`text`](../../types/text/)                | [`numeric`](../../types/numeric/)             | Explicit
[`text`](../../types/text/)                | [`oid`](../../types/oid/)                     | Explicit
[`text`](../../types/text/)                | [`real`](../../types/real/)                   | Explicit
[`text`](../../types/text/)                | [`time`](../../types/time/)                   | Explicit
[`text`](../../types/text/)                | [`timestamp`](../../types/timestamp/)         | Explicit
[`text`](../../types/text/)                | [`timestamptz`](../../types/timestamp/)       | Explicit
[`text`](../../types/text/)                | [`uint2`](../../types/uint/)                  | Explicit
[`text`](../../types/text/)                | [`uint4`](../../types/uint/)                  | Assignment
[`text`](../../types/text/)                | [`uint8`](../../types/uint/)                  | Assignment
[`text`](../../types/text/)                | [`uuid`](../../types/uuid/)                   | Explicit
[`time`](../../types/time/)                | [`interval`](../../types/interval/)           | Implicit
[`time`](../../types/time/)                | [`text`](../../types/text/)                   | Assignment
[`timestamp`](../../types/timestamp/)      | [`date`](../../types/date/)                   | Assignment
[`timestamp`](../../types/timestamp/)      | [`text`](../../types/text/)                   | Assignment
[`timestamp`](../../types/timestamp/)      | [`timestamptz`](../../types/timestamp/)       | Implicit
[`timestamptz`](../../types/timestamp/)    | [`date`](../../types/date/)                   | Assignment
[`timestamptz`](../../types/timestamp/)    | [`text`](../../types/text/)                   | Assignment
[`timestamptz`](../../types/timestamp/)    | [`timestamp`](../../types/timestamp/)         | Assignment
[`uint2`](../../types/uint/)               | [`bigint`](../../types/integer/)              | Implicit
[`uint2`](../../types/uint/)               | [`float`](../../types/float/)                 | Implicit
[`uint2`](../../types/uint/)               | [`int`](../../types/integer/)                 | Implicit
[`uint2`](../../types/uint/)               | [`numeric`](../../types/numeric/)             | Implicit
[`uint2`](../../types/uint/)               | [`real`](../../types/real/)                   | Implicit
[`uint2`](../../types/uint/)               | [`text`](../../types/text/)                   | Assignment
[`uint2`](../../types/uint/)               | [`uint4`](../../types/uint/)                  | Implicit
[`uint2`](../../types/uint/)               | [`uint8`](../../types/uint/)                  | Implicit
[`uint4`](../../types/uint)                | [`bigint`](../../types/integer/)              | Implicit
[`uint4`](../../types/uint)                | [`float`](../../types/float/)                 | Implicit
[`uint4`](../../types/uint)                | [`int`](../../types/integer/)                 | Assignment
[`uint4`](../../types/uint)                | [`numeric`](../../types/numeric/)             | Implicit
[`uint4`](../../types/uint)                | [`real`](../../types/real/)                   | Implicit
[`uint4`](../../types/uint)                | [`text`](../../types/text/)                   | Assignment
[`uint4`](../../types/uint)                | [`uint2`](../../types/uint/)                  | Assignment
[`uint4`](../../types/uint)                | [`uint8`](../../types/uint/)                  | Implicit
[`uint8`](../../types/uint/)               | [`bigint`](../../types/integer/)              | Assignment
[`uint8`](../../types/uint/)               | [`float`](../../types/float/)                 | Implicit
[`uint8`](../../types/uint/)               | [`int`](../../types/integer/)                 | Assignment
[`uint8`](../../types/uint/)               | [`real`](../../types/real/)                   | Implicit
[`uint8`](../../types/uint/)               | [`uint2`](../../types/uint/)                  | Assignment
[`uint8`](../../types/uint/)               | [`uint4`](../../types/uint/)                  | Assignment
[`uuid`](../../types/uuid/)                | [`text`](../../types/text/)                   | Assignment

<sup>1</sup> [`Arrays`](../../types/array/) and [`lists`](../../types/list) are composite types subject to special constraints. See their respective type documentation for details.

<sup>2</sup> Casting a [`float`](../../types/float/) to a [`numeric`](../../types/numeric/) can yield an imprecise result due to the floating point arithmetic involved in the conversion.

## Examples

```mzsql
SELECT INT '4';
```
```nofmt
 ?column?
----------
         4
```

<hr>

```mzsql
SELECT CAST (CAST (100.21 AS numeric(10, 2)) AS float) AS dec_to_float;
```
```nofmt
 dec_to_float
--------------
       100.21
```

<hr/>

```mzsql
SELECT 100.21::numeric(10, 2)::float AS dec_to_float;
```
```nofmt
 dec_to_float
--------------
       100.21
```

## Related topics
* [Data Types](../../types/)


---

## COALESCE function


`COALESCE` returns the first non-`NULL` element provided.

## Signatures

Parameter | Type | Description
----------|------|------------
val | [Any](../../types) | The values you want to check.

### Return value

All elements of the parameters for `coalesce` must be of the same type; `coalesce` returns that type, or _NULL_.

## Examples

```mzsql
SELECT coalesce(NULL, 3, 2, 1) AS coalesce_res;
```
```nofmt
 res
-----
   3
```


---

## csv_extract function


`csv_extract` returns individual component columns from a column containing a CSV file formatted as a string.

## Signatures



```mzsql
csv_extract ( <num_csv_col>, <col_name> )

```

| Syntax element | Description |
| --- | --- |
| `<num_csv_col>` | An [`int`](/sql/types/integer/) value specifying the number of columns in the CSV string.  |
| `<col_name>` | A [`string`](/sql/types/text/) value containing the name of the column containing the CSV string.  |


Parameter | Type | Description
----------|------|------------
_num_csv_col_ | [`int`](../../types/integer/) | The number of columns in the CSV string.
_col_name_  | [`string`](../../types/text/)  | The name of the column containing the CSV string.

### Return value

`EXTRACT` returns [`string`](../../types/text/) columns.

## Example

Create a table where one column is in CSV format and insert some rows:

```mzsql
CREATE TABLE t (id int, data string);
INSERT INTO t
  VALUES (1, 'some,data'), (2, 'more,data'), (3, 'also,data');
```

Extract the component columns from the table column which is a CSV string, sorted by column `id`:

```mzsql
SELECT csv.* FROM t, csv_extract(2, data) csv
  ORDER BY t.id;
```
```nofmt
 column1 | column2
---------+---------
 also    | data
 more    | data
 some    | data
(3 rows)
```


---

## date_bin function


`date_bin` returns the largest value less than or equal to `source` that is a
multiple of `stride` starting at `origin`––for shorthand, we call this
"binning."

For example, on this number line of abstract units:

```nofmt
          x
...|---|---|---|...
   7   8   9   10
```

With a `stride` of 1, we would have bins (...7, 8, 9, 10...).

Here are some example results:

`source` | `origin` | `stride`  | Result
---------|----------|-----------|-------
8.75     | 1        | 1 unit    | 8
8.75     | 1        | 2 units   | 7
8.75     | 1.75     | 1.5 units | 7.75

`date_bin` is similar to [`date_trunc`], but supports arbitrary
strides, rather than only unit times.

## Signatures



```mzsql
date_bin ( <stride>, <source> [, <origin>] )

```

| Syntax element | Description |
| --- | --- |
| `<stride>` | An [`interval`](/sql/types/interval/) value defining the width of bins. Cannot contain any years or months, but can exceed 30 days. Must be between 1 and 9,223,372,036 seconds.  |
| `<source>` | A [`timestamp`](/sql/types/timestamp/) or [`timestamp with time zone`](/sql/types/timestamp/) value to determine the bin for.  |
| `<origin>` | Optional. Must be the same type as `<source>`. Align bins to this value. If not provided, defaults to the Unix epoch. Cannot be more than 2^63 nanoseconds apart from `<source>`.  |


Parameter | Type | Description
----------|------|------------
_stride_ | [`interval`] | Define bins of this width.
_source_ | [`timestamp`], [`timestamp with time zone`] | Determine this value's bin.
_origin_ | Must be the same as _source_ | Align bins to this value.

### Return value

`date_bin` returns the same type as _source_.

## Details

- `origin` and `source` cannot be more than 2^63 nanoseconds apart.
- `stride` cannot contain any years or months, but e.g. can exceed 30 days.
- `stride` only supports values between 1 and 9,223,372,036 seconds.

## Examples

```mzsql
SELECT
  date_bin(
    '15 minutes',
    timestamp '2001-02-16 20:38:40',
    timestamp '2001-02-16 20:05:00'
  );
```
```nofmt
      date_bin
---------------------
 2001-02-16 20:35:00
```

```mzsql
SELECT
  str,
  "interval",
  date_trunc(str, ts)
    = date_bin("interval"::interval, ts, timestamp '2001-01-01') AS equal
FROM (
  VALUES
  ('week', '7 d'),
  ('day', '1 d'),
  ('hour', '1 h'),
  ('minute', '1 m'),
  ('second', '1 s')
) intervals (str, interval),
(VALUES (timestamp '2020-02-29 15:44:17.71393')) ts (ts);
```
```nofmt
  str   | interval | equal
--------+----------+-------
 day    | 1 d      | t
 hour   | 1 h      | t
 week   | 7 d      | t
 minute | 1 m      | t
 second | 1 s      | t
```

[`date_trunc`]: ../date-trunc
[`interval`]: ../../types/interval
[`timestamp`]: ../../types/timestamp
[`timestamp with time zone`]: ../../types/timestamptz


---

## date_part function


`date_part` returns some time component from a time-based value, such as the year from a Timestamp.
It is mostly functionally equivalent to the function [`EXTRACT`](../extract), except to maintain
PostgreSQL compatibility, `date_part` returns values of type [`float`](../../types/float). This can
result in a loss of precision in certain uses. Using [`EXTRACT`](../extract) is recommended instead.

## Signatures



```mzsql
date_part ( '<time_period>', <val> )

```

| Syntax element | Description |
| --- | --- |
| `'<time_period>'` | The time period to extract. Valid values: `epoch`, `millennium`, `century`, `decade`, `year`, `quarter`, `month`, `week`, `day`, `hour`, `minute`, `second`, `microsecond`, `millisecond`, `dow`, `isodow`, `doy`. See the [Arguments](#arguments) section for synonyms.  |
| `<val>` | A [`time`](/sql/types/time/), [`timestamp`](/sql/types/timestamp/), [`timestamp with time zone`](/sql/types/timestamp/), [`interval`](/sql/types/interval/), or [`date`](/sql/types/date/) value. Values of type [`date`](/sql/types/date/) are first cast to type [`timestamp`](/sql/types/timestamp/).  |


Parameter | Type                                                                                                                                                          | Description
----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|------------
_val_ | [`time`](../../types/time), [`timestamp`](../../types/timestamp), [`timestamp with time zone`](../../types/timestamptz), [`interval`](../../types/interval), [`date`](../../types/date) | The value from which you want to extract a component. vals of type [`date`](../../types/date) are first cast to type [`timestamp`](../../types/timestamp).

### Arguments

`date_part` supports multiple synonyms for most time periods.

Time period | Synonyms
------------|---------
epoch | `EPOCH`
millennium   | `MIL`, `MILLENNIUM`, `MILLENNIA`
century | `C`, `CENT`, `CENTURY`, `CENTURIES`
decade  |  `DEC`, `DECS`, `DECADE`, `DECADES`
year | `Y`, `YEAR`, `YEARS`, `YR`, `YRS`
quarter  | `QTR`, `QUARTER`
month | `MON`, `MONS`, `MONTH`, `MONTHS`
week | `W`, `WEEK`, `WEEKS`
day  | `D`, `DAY`, `DAYS`
hour   |`H`, `HR`, `HRS`, `HOUR`, `HOURS`
minute | `M`, `MIN`, `MINS`, `MINUTE`, `MINUTES`
second | `S`, `SEC`, `SECS`, `SECOND`, `SECONDS`
microsecond  | `US`, `USEC`, `USECS`, `USECONDS`, `MICROSECOND`, `MICROSECONDS`
millisecond | `MS`, `MSEC`, `MSECS`, `MSECONDS`, `MILLISECOND`, `MILLISECONDS`
day of week |`DOW`
ISO day of week | `ISODOW`
day of year | `DOY`

### Return value

`date_part` returns a [`float`](../../types/float) value.

## Examples

### Extract second from timestamptz

```mzsql
SELECT date_part('S', TIMESTAMP '2006-01-02 15:04:05.06');
```
```nofmt
 date_part
-----------
      5.06
```

### Extract century from date

```mzsql
SELECT date_part('CENTURIES', DATE '2006-01-02');
```
```nofmt
 date_part
-----------
        21
```


---

## date_trunc function


`date_trunc` computes _ts_val_'s "floor value" of the specified time component,
i.e. the largest time component less than or equal to the provided value.

To align values along arbitrary values, see [`date_bin`].

## Signatures



```mzsql
date_trunc ( '<time_unit>', <val> )

```

| Syntax element | Description |
| --- | --- |
| `'<time_unit>'` | The time unit to truncate to. Valid values: `microseconds`, `milliseconds`, `second`, `minute`, `hour`, `day`, `week`, `month`, `quarter`, `year`, `decade`, `century`, `millenium`.  |
| `<val>` | A [`timestamp`](/sql/types/timestamp/), [`timestamp with time zone`](/sql/types/timestamp/), or [`interval`](/sql/types/interval/) value to truncate.  |


Parameter | Type | Description
----------|------|------------
_val_ | [`timestamp`], [`timestamp with time zone`], [`interval`] | The value you want to truncate.

### Return value

`date_trunc` returns the same type as _val_.

## Examples

```mzsql
SELECT date_trunc('hour', TIMESTAMP '2019-11-26 15:56:46.241150') AS hour_trunc;
```
```nofmt
          hour_trunc
-------------------------------
 2019-11-26 15:00:00.000000000
```

```mzsql
SELECT date_trunc('year', TIMESTAMP '2019-11-26 15:56:46.241150') AS year_trunc;
```
```nofmt
          year_trunc
-------------------------------
 2019-01-01 00:00:00.000000000
```

```mzsql
SELECT date_trunc('millennium', INTERVAL '1234 years 11 months 23 days 23:59:12.123456789') AS millennium_trunc;
```
```nofmt
          millennium_trunc
-------------------------------
 1000 years
```

[`date_bin`]: ../date-bin
[`interval`]: ../../types/interval/
[`timestamp`]: ../../types/timestamp
[`timestamp with time zone`]: ../../types/timestamptz


---

## datediff function


The `datediff(datepart, start, end)` function returns the difference between two date, time or timestamp expressions based on the specified date or time part.

## Signatures

Parameter | Type | Description
----------|------|------------
_datepart_ | [text](../../types) | The date or time part to return. Must be one of [`datepart` specifiers](#datepart-specifiers).
_start_ | [date](../../types), [time](../../types), [timestamp](../../types), [timestamptz](../../types) | The date, time, or timestamp expression to start measuring from.
_end_ | [date](../../types), [time](../../types), [timestamp](../../types), [timestamptz](../../types) | The date, time, or timestamp expression to measuring until.

### `datepart` specifiers

| Specifier                                                        | Description      |
|------------------------------------------------------------------|------------------|
| `millenniums`, `millennium`, `millennia`, `mil`                  | Millennia        |
| `centuries`, `century`, `cent`, `c`                              | Centuries        |
| `decades`, `decade`, `decs`, `dec`                               | Decades          |
| `years`, `year`, `yrs`, `yr`, `y`                                | Years            |
| `quarter`, `qtr`                                                 | Quarters         |
| `months`, `month`, `mons`, `mon`                                 | Months           |
| `weeks`, `week`, `w`                                             | Weeks            |
| `days`, `day`, `d`                                               | Days             |
| `hours`, `hour`, `hrs`, `hr`, `h`                                | Hours            |
| `minutes`, `minute`, `mins`, `min`, `m`                          | Minutes          |
| `seconds`, `second`, `secs`, `sec`, `s`                          | Seconds          |
| `milliseconds`, `millisecond`, `mseconds`, `msecs`, `msec`, `ms` | Milliseconds     |
| `microseconds`, `microsecond`, `useconds`, `usecs`, `usec`, `us` | Microseconds     |

## Examples

To calculate the difference between two dates in millennia:

```
SELECT datediff('millennia', '2000-12-31', '2001-01-01') as d;
  d
-----
  1
```

Even though the difference between `2000-12-31` and `2001-01-01` is a single day, a millennium boundary is crossed from one date to the other, so the result is `1`.

To see how this function handles leap years:
```
SELECT datediff('day', '2004-02-28', '2004-03-01') as leap;
    leap
------------
     2

SELECT datediff('day', '2005-02-28', '2005-03-01') as non_leap;
  non_leap
------------
     1
```

In the statement that uses a leap year (`2004`), the number of day boundaries crossed is `2`. When using a non-leap year (`2005`), only `1` day boundary is crossed.


---

## encode and decode functions


The `encode` function encodes binary data into one of several textual
representations. The `decode` function does the reverse.

## Signatures

```
encode(b: bytea, format: text) -> text
decode(s: text, format: text) -> bytea
```

## Details

### Supported formats

The following formats are supported by both `encode` and `decode`.

#### `base64`

The `base64` format is defined in [Section 6.8 of RFC 2045][rfc2045].

To comply with the RFC, the `encode` function inserts a newline (`\n`) after
every 76 characters. The `decode` function ignores any whitespace in its input.

#### `escape`

The `escape` format renders zero bytes and bytes with the high bit set (`0x80` -
`0xff`) as an octal escape sequence (`\nnn`), renders backslashes as `\\`, and
renders all other characters literally. The `decode` function rejects invalid
escape sequences (e.g., `\9` or `\a`).

#### `hex`

The `hex` format represents each byte of input as two hexadecimal digits, with
the most significant digit first. The `encode` function uses lowercase for the
`a`-`f` digits, with no whitespace between digits. The `decode` function accepts
lowercase or uppercase for the `a` - `f` digits and permits whitespace between
each encoded byte, though not within a byte.

## Examples

Encoding and decoding in the `base64` format:

```mzsql
SELECT encode('\x00404142ff', 'base64');
```
```nofmt
  encode
----------
 AEBBQv8=
```

```mzsql
SELECT decode('A   EB BQv8 =', 'base64');
```
```nofmt
    decode
--------------
 \x00404142ff
```

```mzsql
SELECT encode('This message is long enough that the output will run to multiple lines.', 'base64');
```
```nofmt
                                    encode
------------------------------------------------------------------------------
 VGhpcyBtZXNzYWdlIGlzIGxvbmcgZW5vdWdoIHRoYXQgdGhlIG91dHB1dCB3aWxsIHJ1biB0byBt+
 dWx0aXBsZSBsaW5lcy4=
```

<hr>

Encoding and decoding in the `escape` format:

```mzsql
SELECT encode('\x00404142ff', 'escape');
```
```nofmt
   encode
-------------
 \000@AB\377
```

```mzsql
SELECT decode('\000@AB\377', 'escape');
```
```nofmt
    decode
--------------
 \x00404142ff
```

<hr>

Encoding and decoding in the `hex` format:

```mzsql
SELECT encode('\x00404142ff', 'hex');
```
```nofmt
   encode
------------
 00404142ff
```

```mzsql
SELECT decode('00  40  41  42  ff', 'hex');
```
```nofmt
    decode
--------------
 \x00404142ff
```

[rfc2045]: https://tools.ietf.org/html/rfc2045#section-6.8


---

## EXTRACT function


`EXTRACT` returns some time component from a time-based value, such as the year from a Timestamp.

## Signatures



```mzsql
EXTRACT ( <time_period> FROM <val> )

```

| Syntax element | Description |
| --- | --- |
| `<time_period>` | The time period to extract. Valid values: `EPOCH`, `MILLENNIUM`, `CENTURY`, `DECADE`, `YEAR`, `QUARTER`, `MONTH`, `WEEK`, `DAY`, `HOUR`, `MINUTE`, `SECOND`, `MICROSECOND`, `MILLISECOND`, `DOW`, `ISODOW`, `DOY`. See the [Arguments](#arguments) section for synonyms.  |
| **FROM** `<val>` | A [`date`](/sql/types/date/), [`time`](/sql/types/time/), [`timestamp`](/sql/types/timestamp/), [`timestamp with time zone`](/sql/types/timestamp/), or [`interval`](/sql/types/interval/) value from which to extract a component.  |


Parameter | Type                                                                                                                                                                                    | Description
----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------
_val_ | [`date`](../../types/date), [`time`](../../types/time), [`timestamp`](../../types/timestamp), [`timestamp with time zone`](../../types/timestamptz), [`interval`](../../types/interval) | The value from which you want to extract a component.

### Arguments

`EXTRACT` supports multiple synonyms for most time periods.

Time period | Synonyms
------------|---------
epoch | `EPOCH`
millennium   | `MIL`, `MILLENNIUM`, `MILLENNIA`
century | `C`, `CENT`, `CENTURY`, `CENTURIES`
decade  |  `DEC`, `DECS`, `DECADE`, `DECADES`
 year | `Y`, `YEAR`, `YEARS`, `YR`, `YRS`
 quarter  | `QTR`, `QUARTER`
 month | `MON`, `MONS`, `MONTH`, `MONTHS`
 week | `W`, `WEEK`, `WEEKS`
 day  | `D`, `DAY`, `DAYS`
 hour   |`H`, `HR`, `HRS`, `HOUR`, `HOURS`
 minute | `M`, `MIN`, `MINS`, `MINUTE`, `MINUTES`
 second | `S`, `SEC`, `SECS`, `SECOND`, `SECONDS`
 microsecond  | `US`, `USEC`, `USECS`, `USECONDS`, `MICROSECOND`, `MICROSECONDS`
 millisecond | `MS`, `MSEC`, `MSECS`, `MSECONDS`, `MILLISECOND`, `MILLISECONDS`
 day of week |`DOW`
 ISO day of week | `ISODOW`
 day of year | `DOY`

### Return value

`EXTRACT` returns a [`numeric`](../../types/numeric) value.

## Examples

### Extract second from timestamptz

```mzsql
SELECT EXTRACT(S FROM TIMESTAMP '2006-01-02 15:04:05.06');
```
```nofmt
 extract
---------
    5.06
```

### Extract century from date

```mzsql
SELECT EXTRACT(CENTURIES FROM DATE '2006-01-02');
```
```nofmt
 extract
---------
      21
```


---

## jsonb_agg function


The `jsonb_agg(expression)` function aggregates all values indicated by its expression,
returning the values (including nulls) as a [`jsonb`](/sql/types/jsonb) array.
The input values to the aggregate can be [filtered](../filters).

## Syntax



```mzsql
jsonb_agg ( <expression>
  [ORDER BY <col_ref> [ASC | DESC] [NULLS FIRST | NULLS LAST] [, ...]]
)
[FILTER (WHERE <filter_clause>)]

```

| Syntax element | Description |
| --- | --- |
| `<expression>` | The values you want aggregated.  |
| **ORDER BY** `<col_ref>` [**ASC** \| **DESC**] [**NULLS FIRST** \| **NULLS LAST**] [, ...] | Optional. Specifies the ordering of values within the aggregation. If not specified, incoming rows are not guaranteed any order.  |
| **FILTER** (WHERE `<filter_clause>`) | Optional. Specifies which rows are sent to the aggregate function. Rows for which the `<filter_clause>` evaluates to true contribute to the aggregation. See [Aggregate function filters](/sql/functions/filters) for details.  |


## Signatures

Parameter | Type | Description
----------|------|------------
_expression_ | [jsonb](../../types) | The values you want aggregated.

### Return value

`jsonb_agg` returns the aggregated values as a `jsonb` array.

This function always executes on the data from `value` as if it were sorted in ascending order before the function call. Any specified ordering is
ignored. If you need to perform aggregation in a specific order, you must specify `ORDER BY` within the aggregate function call itself. Otherwise incoming rows are not guaranteed any order.

## Details

### Usage in dataflows

While `jsonb_agg` is available in Materialize, materializing `jsonb_agg(expression)`
is considered an incremental view maintenance anti-pattern. Any change to the data
underlying the function call will require the function to be recomputed entirely,
discarding the benefits of maintaining incremental updates.

Instead, we recommend that you materialize all components required for the
`jsonb_agg` function call and create a non-materialized view using `jsonb_agg`
on top of that. That pattern is illustrated in the following statements:

```mzsql
CREATE MATERIALIZED VIEW foo_view AS SELECT * FROM foo;
CREATE VIEW bar AS SELECT jsonb_agg(foo_view.bar) FROM foo_view;
```

## Examples

```mzsql
SELECT
  jsonb_agg(t) FILTER (WHERE t.content LIKE 'h%')
    AS my_agg
FROM (
  VALUES
  (1, 'hey'),
  (2, NULL),
  (3, 'hi'),
  (4, 'salutations')
  ) AS t(id, content);
```
```nofmt
                       my_agg
----------------------------------------------------
 [{"content":"hi","id":3},{"content":"hey","id":1}]
```

## See also

* [`jsonb_object_agg`](/sql/functions/jsonb_object_agg)


---

## jsonb_object_agg function


The `jsonb_object_agg(keys, values)` aggregate function zips together `keys`
and `values` into a [`jsonb`](/sql/types/jsonb) object.
The input values to the aggregate can be [filtered](../filters).

## Syntax



```mzsql
jsonb_object_agg ( <keys>, <values>
  [ORDER BY <col_ref> [ASC | DESC] [NULLS FIRST | NULLS LAST] [, ...]]
)
[FILTER (WHERE <filter_clause>)]

```

| Syntax element | Description |
| --- | --- |
| `<keys>` | The keys to aggregate.  |
| `<values>` | The values to aggregate.  |
| **ORDER BY** `<col_ref>` [**ASC** \| **DESC**] [**NULLS FIRST** \| **NULLS LAST**] [, ...] | Optional. Specifies the ordering of values within the aggregation. If not specified, incoming rows are not guaranteed any order.  |
| **FILTER** (WHERE `<filter_clause>`) | Optional. Specifies which rows are sent to the aggregate function. Rows for which the `<filter_clause>` evaluates to true contribute to the aggregation. See [Aggregate function filters](/sql/functions/filters) for details.  |


## Signatures

Parameter | Type | Description
----------|------|------------
_keys_    | any  | The keys to aggregate.
_values_  | any  | The values to aggregate.

### Return value

`jsonb_object_agg` returns the aggregated key–value pairs as a jsonb object.
Each row in the input corresponds to one key–value pair in the output.

If there are duplicate keys in the input, it is unspecified which key–value
pair is retained in the output.

If `keys` is null for any input row, that entry pair will be dropped.

This function always executes on the data from `value` as if it were sorted in ascending order before the function call. Any specified ordering is
ignored. If you need to perform aggregation in a specific order, you must specify `ORDER BY` within the aggregate function call itself. Otherwise incoming rows are not guaranteed any order.

### Usage in dataflows

While `jsonb_object_agg` is available in Materialize, materializing
`jsonb_object_agg(expression)` is considered an incremental view maintenance
anti-pattern. Any change to the data underlying the function call will require
the function to be recomputed entirely, discarding the benefits of maintaining
incremental updates.

Instead, we recommend that you materialize all components required for the
`jsonb_object_agg` function call and create a non-materialized view using
`jsonb_object_agg` on top of that. That pattern is illustrated in the following
statements:

```mzsql
CREATE MATERIALIZED VIEW foo_view AS SELECT key_col, val_col FROM foo;
CREATE VIEW bar AS SELECT jsonb_object_agg(key_col, val_col) FROM foo_view;
```

## Examples

Consider this query:
```mzsql
SELECT
  jsonb_object_agg(
    t.col1,
    t.col2
    ORDER BY t.ts ASC
  ) FILTER (WHERE t.col2 IS NOT NULL) AS my_agg
FROM (
  VALUES
  ('k1', 1, now()),
  ('k2', 2, now() - INTERVAL '1s'),
  ('k2', -1, now()),
  ('k2', NULL, now() + INTERVAL '1s')
  ) AS t(col1, col2, ts);
```
```nofmt
      my_agg
------------------
 {"k1": 1, "k2": -1}
```
In this example, there are multiple values associated with the `k2` key.

The `FILTER` clause in the statement above returns values that are not `NULL` and orders them by the timestamp column to return the most recent associated value.

## See also

* [`jsonb_agg`](/sql/functions/jsonb_agg)


---

## justify_days function


`justify_days` returns a new [`interval`](../../types/interval) such that 30-day time periods are
converted to months.

## Signatures



```mzsql
justify_days ( <interval> )

```

| Syntax element | Description |
| --- | --- |
| `<interval>` | An [`interval`](/sql/types/interval/) value to justify. Returns a new interval such that 30-day time periods are converted to months.  |


Parameter | Type                                                                                                                                                                                            | Description
----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------
_interval_ | [`interval`](../../types/interval) | The interval value to justify.


### Return value

`justify_days` returns an [`interval`](../../types/interval) value.

## Example

```mzsql
SELECT justify_days(interval '35 days');
```
```nofmt
  justify_days
----------------
 1 month 5 days
```


---

## justify_hours function


`justify_hours` returns a new [`interval`](../../types/interval) such that 24-hour time periods are
converted to days.

## Signatures



```mzsql
justify_hours ( <interval> )

```

| Syntax element | Description |
| --- | --- |
| `<interval>` | An [`interval`](/sql/types/interval/) value to justify. Returns a new interval such that 24-hour time periods are converted to days.  |


Parameter | Type                                                                                                                                                                                            | Description
----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------
_interval_ | [`interval`](../../types/interval) | The interval value to justify.


### Return value

`justify_hours` returns an [`interval`](../../types/interval) value.

## Example

```mzsql
SELECT justify_hours(interval '27 hours');
```
```nofmt
 justify_hours
----------------
 1 day 03:00:00
```


---

## justify_interval function


`justify_interval` returns a new [`interval`](../../types/interval) such that 30-day time periods are
converted to months, 24-hour time periods are represented as days, and all fields have the same sign. It is a
combination of ['justify_days'](../justify-days) and ['justify_hours'](../justify-hours) with additional sign
adjustment.

## Signatures



```mzsql
justify_interval ( <interval> )

```

| Syntax element | Description |
| --- | --- |
| `<interval>` | An [`interval`](/sql/types/interval/) value to justify. Returns a new interval such that 30-day time periods are converted to months, 24-hour time periods are represented as days, and all fields have the same sign. It is a combination of [`justify_days`](/sql/functions/justify-days) and [`justify_hours`](/sql/functions/justify-hours) with additional sign adjustment.  |


Parameter | Type                                                                                                                                                                                            | Description
----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------
_interval_ | [`interval`](../../types/interval) | The interval value to justify.


### Return value

`justify_interval` returns an [`interval`](../../types/interval) value.

## Example

```mzsql
SELECT justify_interval(interval '1 mon -1 hour');
```
```nofmt
 justify_interval
------------------
 29 days 23:00:00
```


---

## LENGTH function


`LENGTH` returns the [code points](https://en.wikipedia.org/wiki/Code_point) in
an encoded string.

## Signatures



```mzsql
length ( <str> [, <encoding_name>] )

```

| Syntax element | Description |
| --- | --- |
| `<str>` | A [`string`](/sql/types/text/) or `bytea` value whose length you want.  |
| `<encoding_name>` | Optional. A [`string`](/sql/types/text/) value specifying the [encoding](#encoding-details) to use for calculating the string's length. Defaults to UTF-8.  |


Parameter | Type | Description
----------|------|------------
_str_ | [`string`](../../types/string) or `bytea` | The string whose length you want.
_encoding&lowbar;name_ | [`string`](../../types/string) | The [encoding](#encoding-details) you want to use for calculating the string's length. _Defaults to UTF-8_.

### Return value

`length` returns an [`int`](../../types/int).

## Details

### Errors

`length` operations might return `NULL` values indicating errors in the
following cases:

- The _encoding&lowbar;name_ provided is not available in our encoding package.
- Some byte sequence in _str_ was not compatible with the selected encoding.

### Encoding details

- Materialize uses the [`encoding`](https://crates.io/crates/encoding) crate.
  See the [list of supported
  encodings](https://lifthrasiir.github.io/rust-encoding/encoding/index.html#supported-encodings),
  as well as their names [within the
  API](https://github.com/lifthrasiir/rust-encoding/blob/4e79c35ab6a351881a86dbff565c4db0085cc113/src/label.rs).
- Materialize attempts to convert [PostgreSQL-style encoding
  names](https://www.postgresql.org/docs/9.5/multibyte.html) into the
  [WHATWG-style encoding names](https://encoding.spec.whatwg.org/) used by the
  API.

    For example, you can refer to `iso-8859-5` (WHATWG-style) as `ISO_8859_5`
    (PostrgreSQL-style).

    However, there are some differences in the names of the same encodings that
    we do not convert. For example, the
    [windows-874](https://encoding.spec.whatwg.org/#windows-1252) encoding is
    referred to as `WIN874` in PostgreSQL; Materialize does not convert these names.

## Examples

```mzsql
SELECT length('你好') AS len;
```
```nofmt
 len
-----
   2
```

<hr/>

```mzsql
SELECT length('你好', 'big5') AS len;
```
```nofmt
 len
-----
   3
```


---

## list_agg function


The `list_agg(value)` aggregate function concatenates
input values (including nulls) into a [`list`](/sql/types/list).
The input values to the aggregate can be [filtered](../filters).

## Syntax



```mzsql
list_agg ( <value>
  [ORDER BY <col_ref> [ASC | DESC] [NULLS FIRST | NULLS LAST] [, ...]]
)
[FILTER (WHERE <filter_clause>)]

```

| Syntax element | Description |
| --- | --- |
| `<value>` | The values to concatenate.  |
| **ORDER BY** `<col_ref>` [**ASC** \| **DESC**] [**NULLS FIRST** \| **NULLS LAST**] [, ...] | Optional. Specifies the ordering of values within the aggregation. If not specified, incoming rows are not guaranteed any order.  |
| **FILTER** (WHERE `<filter_clause>`) | Optional. Specifies which rows are sent to the aggregate function. Rows for which the `<filter_clause>` evaluates to true contribute to the aggregation. See [Aggregate function filters](/sql/functions/filters) for details.  |


## Signatures

Parameter | Type | Description
----------|------|------------
_value_    | `text`  | The values to concatenate.

### Return value

`list_agg` returns a [`list`](/sql/types/list) value.

This function always executes on the data from `value` as if it were sorted in ascending order before the function call. Any specified ordering is
ignored. If you need to perform aggregation in a specific order, you must specify `ORDER BY` within the aggregate function call itself. Otherwise incoming rows are not guaranteed any order.

## Details

### Usage in dataflows

While `list_agg` is available in Materialize, materializing `list_agg(values)`
is considered an incremental view maintenance anti-pattern. Any change to the data
underlying the function call will require the function to be recomputed entirely,
discarding the benefits of maintaining incremental updates.

Instead, we recommend that you materialize all components required for the
`list_agg` function call and create a non-materialized view using `list_agg`
on top of that. That pattern is illustrated in the following statements:

```mzsql
CREATE MATERIALIZED VIEW foo_view AS SELECT * FROM foo;
CREATE VIEW bar AS SELECT list_agg(foo_view.bar) FROM foo_view;
```

## Examples

```mzsql
SELECT
    title,
    LIST_AGG (
        first_name || ' ' || last_name
        ORDER BY
            last_name
    ) actors
FROM
    film
GROUP BY
    title;
```


---

## map_agg function


The `map_agg(keys, values)` aggregate function zips together `keys`
and `values` into a [`map`](/sql/types/map).

The input values to the aggregate can be [filtered](../filters).

## Syntax



```mzsql
map_agg ( <keys>, <values>
  [ORDER BY <col_ref> [ASC | DESC] [NULLS FIRST | NULLS LAST] [, ...]]
)
[FILTER (WHERE <filter_clause>)]

```

| Syntax element | Description |
| --- | --- |
| `<keys>` | The keys to aggregate. Must be of type [`text`](/sql/types/text/).  |
| `<values>` | The values to aggregate.  |
| **ORDER BY** `<col_ref>` [**ASC** \| **DESC**] [**NULLS FIRST** \| **NULLS LAST**] [, ...] | Optional. Specifies the ordering of values within the aggregation. If not specified, incoming rows are not guaranteed any order.  |
| **FILTER** (WHERE `<filter_clause>`) | Optional. Specifies which rows are sent to the aggregate function. Rows for which the `<filter_clause>` evaluates to true contribute to the aggregation. See [Aggregate function filters](/sql/functions/filters) for details.  |


## Signatures

| Parameter | Type                       | Description              |
| --------- | -------------------------- | ------------------------ |
| _keys_    | [`text`](/sql/types/text/) | The keys to aggregate.   |
| _values_  | any                        | The values to aggregate. |

### Return value

`map_agg` returns the aggregated key–value pairs as a map.

-   Each row in the input corresponds to one key–value pair in the output,
    unless the `key` is null, in which case the pair is ignored. (`map` keys
    must be non-`NULL` strings.)
-   If multiple rows have the same key, we retain only the value sorted in the
    greatest/last position. You can determine this order using `ORDER BY` within
    the aggregate function itself; otherwise, incoming rows are not guaranteed
    to be handled in any order.

### Usage in dataflows

While `map_agg` is available in Materialize, materializing
`map_agg(expression)` is considered an incremental view maintenance
anti-pattern. Any change to the data underlying the function call will require
the function to be recomputed entirely, discarding the benefits of maintaining
incremental updates.

Instead, we recommend that you materialize all components required for the
`map_agg` function call and create a non-materialized view using
`map_agg` on top of that. That pattern is illustrated in the following
statements:

```mzsql
CREATE MATERIALIZED VIEW foo_view AS SELECT key_col, val_col FROM foo;
CREATE VIEW bar AS SELECT map_agg(key_col, val_col) FROM foo_view;
```

## Examples

Consider this query:

```mzsql
SELECT
  map_agg(
    t.k,
    t.v
    ORDER BY t.ts ASC, t.v DESC
  ) FILTER (WHERE t.v != -8) AS my_agg
FROM (
  VALUES
    -- k1
    ('k1', 3, now()),
    ('k1', 2, now() + INTERVAL '1s'),
    ('k1', 1, now() + INTERVAL '1s'),
    -- k2
    ('k2', -9, now() - INTERVAL '1s'),
    ('k2', -8, now()),
    ('k2', NULL, now() + INTERVAL '1s'),
    -- null
    (NULL, 99, now()),
    (NULL, 100, now())
  ) AS t(k, v, ts);
```

```nofmt
      my_agg
------------------
 {k1=>1,k2=>-9}
```

In this example:

-   We order values by their timestamp (`t.ts ASC`) and then break ties using the
    smallest values (`t.v DESC`).
-   We filter out any values equal to exactly `-8`.
-   All keys with a `NULL` value get excluded automatically.
-   `k1` has two values tied with the same `t.ts` value; because we've also
    ordered `t.v DESC`, the last value we see will be `1`.
-   `k2` has its value for `-8` filtered out `FILTER (WHERE t.v != -8)`;
    however, this `FILTER` also removes the `NULL` value at `now() + INTERVAL '1s'` because `WHERE null != -8` evaluates to `false`.


---

## normalize function


`normalize` converts a string to a specified Unicode normalization form.

## Signatures

<no value>```mzsql
normalize(str)
normalize(str, form)

```

Parameter | Type | Description
----------|------|------------
_str_ | [`string`](../../types/string) | The string to normalize.
_form_ | keyword | The Unicode normalization form: `NFC`, `NFD`, `NFKC`, or `NFKD` (unquoted, case-insensitive keywords). _Defaults to `NFC`_.

### Return value

`normalize` returns a [`string`](../../types/string).

## Details

Unicode normalization is a process that converts different binary representations of characters to a canonical form. This is useful when comparing strings that may have been encoded differently.

The four normalization forms are:

- **NFC** (Normalization Form Canonical Composition): Canonical decomposition, followed by canonical composition. This is the default and most commonly used form.
- **NFD** (Normalization Form Canonical Decomposition): Canonical decomposition only. Characters are decomposed into their constituent parts.
- **NFKC** (Normalization Form Compatibility Composition): Compatibility decomposition, followed by canonical composition. This applies more aggressive transformations, converting compatibility variants to standard forms.
- **NFKD** (Normalization Form Compatibility Decomposition): Compatibility decomposition only.

For more information, see:
- [Unicode Normalization Forms](https://unicode.org/reports/tr15/#Norm_Forms)
- [PostgreSQL normalize function](https://www.postgresql.org/docs/current/functions-string.html)

## Examples

Normalize a string using the default NFC form:
```mzsql
SELECT normalize('é') AS normalized;

``````nofmt
 normalized
------------
 é
```


<hr/>

NFC combines base character with combining marks:
```mzsql
SELECT normalize('é', NFC) AS nfc;

``````nofmt
 nfc
-----
 é
```


<hr/>

NFD decomposes into base character + combining accent:
```mzsql
SELECT normalize('é', NFD) = E'e\u0301' AS is_decomposed;

``````nofmt
 is_decomposed
---------------
 true
```


<hr/>

NFKC decomposes compatibility characters like ligatures:
```mzsql
SELECT normalize('ﬁ', NFKC) AS decomposed;

``````nofmt
 decomposed
------------
 fi
```


<hr/>

NFKC converts superscripts to regular characters:
```mzsql
SELECT normalize('x²', NFKC) AS normalized;

``````nofmt
 normalized
------------
 x2
```



---

## now and mz_now functions


In Materialize, `now()` returns the value of the system clock when the
transaction began as a [`timestamp with time zone`] value.

By contrast, `mz_now()` returns the logical time at which the query was executed
as a [`mz_timestamp`] value.

## Details

### `mz_now()` clause

<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="n">mz_now</span><span class="p">()</span> <span class="o">&lt;</span><span class="n">comparison_operator</span><span class="o">&gt;</span> <span class="o">&lt;</span><span class="n">numeric_expr</span> <span class="o">|</span> <span class="n">timestamp_expr</span><span class="o">&gt;</span>
</span></span></code></pre></div><ul>
<li>
<code>mz_now()</code> must be used with one of the following comparison operators: <code>=</code>,
<code>&lt;</code>, <code>&lt;=</code>, <code>&gt;</code>, <code>&gt;=</code>, or an operator that desugars to them or to a conjunction
(<code>AND</code>) of them (for example, <code>BETWEEN...AND...</code>). That is, you cannot use
date/time operations directly on  <code>mz_now()</code> to calculate a timestamp in the
past or future. Instead, rewrite the query expression to move the operation to
the other side of the comparison.
</li>
<li>
<p><code>mz_now()</code> can only be compared to either a
<a href="/sql/types/numeric" ><code>numeric</code></a> expression or a
<a href="/sql/types/timestamp" ><code>timestamp</code></a> expression not containing <code>mz_now()</code>.</p>
</li>
</ul>

### Usage patterns

The typical uses of `now()` and `mz_now()` are:

* **Temporal filters**

  You can use `mz_now()` in a `WHERE` or `HAVING` clause to limit the working dataset.
  This is referred to as a **temporal filter**.
  See the [temporal filter](/sql/patterns/temporal-filters) pattern for more details.

* **Query timestamp introspection**

  An ad hoc `SELECT` query with `now()` and `mz_now()` can be useful if you need to understand how up to date the data returned by a query is.
  The data returned by the query reflects the results as of the logical time returned by a call to `mz_now()` in that query.

### Logical timestamp selection

When using the [serializable](/get-started/isolation-level#serializable)
isolation level, the logical timestamp may be arbitrarily ahead of or behind the
system clock. For example, at a wall clock time of 9pm, Materialize may choose
to execute a serializable query as of logical time 8:30pm, perhaps because data
for 8:30–9pm has not yet arrived. In this scenario, `now()` would return 9pm,
while `mz_now()` would return 8:30pm.

When using the [strict serializable](/get-started/isolation-level#strict-serializable)
isolation level, Materialize attempts to keep the logical timestamp reasonably
close to wall clock time. In most cases, the logical timestamp of a query will
be within a few seconds of the wall clock time. For example, when executing
a strict serializable query at a wall clock time of 9pm, Materialize will choose
a logical timestamp within a few seconds of 9pm, even if data for 8:30–9pm has
not yet arrived and the query will need to block until the data for 9pm arrives.
In this scenario, both `now()` and `mz_now()` would return 9pm.

### Limitations

#### Materialization

* Queries that use `now()` cannot be materialized. In other words, you cannot
  create an index or a materialized view on a query that calls `now()`.

* Queries that use `mz_now()` can only be materialized if the call to
  `mz_now()` is used in a [temporal filter](/sql/patterns/temporal-filters).

These limitations are in place because `now()` changes every microsecond and
`mz_now()` changes every millisecond. Allowing these functions to be
materialized would be resource prohibitive.

#### `mz_now()` restrictions

The [`mz_now()`](/sql/functions/now_and_mz_now) clause has the following
restrictions:

- <p>When used in a materialized view definition, a view definition that is being
indexed (i.e., although you can create the view and perform ad-hoc query on
the view, you cannot create an index on that view), or a <code>SUBSCRIBE</code>
statement:</p>
<ul>
<li>
<p><code>mz_now()</code> clauses can only be combined using an <code>AND</code>, and</p>
</li>
<li>
<p>All top-level <code>WHERE</code> or <code>HAVING</code> conditions must be combined using an <code>AND</code>,
even if the <code>mz_now()</code> clause is nested.</p>
</li>
</ul>


  For example:


  | mz_now() Compound Clause | Valid/Invalid |
  | --- | --- |
  | <span class="copyableCode"> <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders</span> </span></span><span class="line"><span class="cl"><span class="k">WHERE</span> <span class="n">status</span> <span class="o">=</span> <span class="s1">&#39;Shipped&#39;</span> </span></span><span class="line"><span class="cl"><span class="k">OR</span> <span class="n">order_date</span> <span class="o">+</span> <span class="nb">interval</span> <span class="s1">&#39;1&#39;</span> <span class="k">days</span> <span class="o">&lt;=</span> <span class="n">mz_now</span><span class="p">()</span> </span></span><span class="line"><span class="cl"><span class="p">;</span> </span></span></code></pre></div></span>  | <p>✅ <strong>Valid</strong></p> <p>Ad-hoc queries do not have the same restrictions.</p>  |
  | <span class="copyableCode"> <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">MATERIALIZED</span> <span class="k">VIEW</span> <span class="n">forecast_completed_orders</span> <span class="k">AS</span> </span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders</span> </span></span><span class="line"><span class="cl"><span class="k">WHERE</span> <span class="n">order_date</span> <span class="o">+</span> <span class="nb">interval</span> <span class="s1">&#39;3&#39;</span> <span class="k">days</span> <span class="o">&gt;</span> <span class="n">mz_now</span><span class="p">()</span> </span></span><span class="line"><span class="cl"><span class="k">AND</span> <span class="n">order_date</span> <span class="o">+</span> <span class="nb">interval</span> <span class="s1">&#39;1&#39;</span> <span class="k">days</span> <span class="o">&lt;</span> <span class="n">mz_now</span><span class="p">()</span> </span></span><span class="line"><span class="cl"><span class="p">;</span> </span></span></code></pre></div></span>  | ✅ <strong>Valid</strong> |
  | <span class="copyableCode"> <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">MATERIALIZED</span> <span class="k">VIEW</span> <span class="n">forecast_completed_orders</span> <span class="k">AS</span> </span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders</span> </span></span><span class="line"><span class="cl"><span class="k">WHERE</span> <span class="p">(</span><span class="n">status</span> <span class="o">=</span> <span class="s1">&#39;Complete&#39;</span> <span class="k">OR</span> <span class="n">status</span> <span class="o">=</span> <span class="s1">&#39;Shipped&#39;</span><span class="p">)</span> </span></span><span class="line"><span class="cl"><span class="k">AND</span> <span class="n">order_date</span> <span class="o">+</span> <span class="nb">interval</span> <span class="s1">&#39;1&#39;</span> <span class="k">days</span> <span class="o">&lt;=</span> <span class="n">mz_now</span><span class="p">()</span> </span></span><span class="line"><span class="cl"><span class="p">;</span> </span></span></code></pre></div></span>  | ✅ <strong>Valid</strong> |
  | <div style="background-color: var(--code-block)"> <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">MATERIALIZED</span> <span class="k">VIEW</span> <span class="n">forecast_completed_orders</span> <span class="k">AS</span> </span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders</span> </span></span><span class="line"><span class="cl"><span class="k">WHERE</span> <span class="n">status</span> <span class="o">=</span> <span class="s1">&#39;Shipped&#39;</span> </span></span><span class="line"><span class="cl"><span class="k">OR</span> <span class="n">order_date</span> <span class="o">+</span> <span class="nb">interval</span> <span class="s1">&#39;1&#39;</span> <span class="k">days</span> <span class="o">&lt;=</span> <span class="n">mz_now</span><span class="p">()</span> </span></span><span class="line"><span class="cl"><span class="p">;</span> </span></span></code></pre></div></div>  | <p>❌ <strong>Invalid</strong></p> <p>In materialized view definitions, <code>mz_now()</code> clause can only be combined using an <code>AND</code>.</p>  |
  | <div style="background-color: var(--code-block)"> <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">MATERIALIZED</span> <span class="k">VIEW</span> <span class="n">forecast_completed_orders</span> <span class="k">AS</span> </span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders</span> </span></span><span class="line"><span class="cl"><span class="k">WHERE</span> <span class="n">status</span> <span class="o">=</span> <span class="s1">&#39;Complete&#39;</span> </span></span><span class="line"><span class="cl"><span class="k">OR</span> <span class="p">(</span><span class="n">status</span> <span class="o">=</span> <span class="s1">&#39;Shipped&#39;</span> <span class="k">AND</span> <span class="n">order_date</span> <span class="o">+</span> <span class="nb">interval</span> <span class="s1">&#39;1&#39;</span> <span class="k">days</span> <span class="o">&lt;=</span> <span class="n">mz_now</span><span class="p">())</span> </span></span></code></pre></div></div>  | <p>❌ <strong>Invalid</strong></p> <p>In materialized view definitions with <code>mz_now()</code> clauses, top-level conditions must be combined using an <code>AND</code>.</p>  |
  | <div style="background-color: var(--code-block)"> <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">VIEW</span> <span class="n">forecast_completed_orders</span> <span class="k">AS</span> </span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders</span> </span></span><span class="line"><span class="cl"><span class="k">WHERE</span> <span class="n">status</span> <span class="o">=</span> <span class="s1">&#39;Complete&#39;</span> </span></span><span class="line"><span class="cl"><span class="k">OR</span> <span class="p">(</span><span class="n">status</span> <span class="o">=</span> <span class="s1">&#39;Shipped&#39;</span> <span class="k">AND</span> <span class="n">order_date</span> <span class="o">+</span> <span class="nb">interval</span> <span class="s1">&#39;1&#39;</span> <span class="k">days</span> <span class="o">&lt;=</span> <span class="n">mz_now</span><span class="p">())</span> </span></span><span class="line"><span class="cl"><span class="p">;</span> </span></span><span class="line"><span class="cl"> </span></span><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">INDEX</span> <span class="n">idx_forecast_completed_orders</span> <span class="k">ON</span> <span class="n">forecast_completed_orders</span> </span></span><span class="line"><span class="cl"><span class="p">(</span><span class="n">order_date</span><span class="p">);</span> <span class="c1">-- Unsupported because of the `mz_now()` clause </span></span></span></code></pre></div></div>  | <p>❌ <strong>Invalid</strong></p> <p>To index a view whose definitions includes <code>mz_now()</code> clauses, top-level conditions must be combined using an <code>AND</code> in the view definition.</p>  |


  For alternatives, see [Disjunction (OR)
  alternatives](http://localhost:1313/docs/transform-data/idiomatic-materialize-sql/mz_now/#disjunctions-or).

- If part of a  `WHERE` clause, the `WHERE` clause cannot be an [aggregate
 `FILTER` expression](/sql/functions/filters).

## Examples

### Temporal filters

<!-- This example also appears in temporal-filters -->
It is common for real-time applications to be concerned with only a recent period of time.
In this case, we will filter a table to only include records from the last 30 seconds.

```mzsql
-- Create a table of timestamped events.
CREATE TABLE events (
    content TEXT,
    event_ts TIMESTAMP
);

-- Create a view of events from the last 30 seconds.
CREATE VIEW last_30_sec AS
SELECT event_ts, content
FROM events
WHERE mz_now() <= event_ts + INTERVAL '30s';
```

Next, subscribe to the results of the view.

```mzsql
COPY (SUBSCRIBE (SELECT event_ts, content FROM last_30_sec)) TO STDOUT;
```

In a separate session, insert a record.

```mzsql
INSERT INTO events VALUES (
    'hello',
    now()
);
```

Back in the first session, watch the record expire after 30 seconds. Press `Ctrl+C` to quit the `SUBSCRIBE` when you are ready.

```nofmt
1686868190714   1       2023-06-15 22:29:50.711 hello
1686868220712   -1      2023-06-15 22:29:50.711 hello
```

You can materialize the `last_30_sec` view by creating an index on it (results stored in memory) or by recreating it as a `MATERIALIZED VIEW` (results persisted to storage). When you do so, Materialize will keep the results up to date with records expiring automatically according to the temporal filter.

### Query timestamp introspection

If you haven't already done so in the previous example, create a table called `events` and add a few records.

```mzsql
-- Create a table of timestamped events.
CREATE TABLE events (
    content TEXT,
    event_ts TIMESTAMP
);
-- Insert records
INSERT INTO events VALUES (
    'hello',
    now()
);
INSERT INTO events VALUES (
    'welcome',
    now()
);
INSERT INTO events VALUES (
    'goodbye',
    now()
);
```

Execute this ad hoc query that adds the current system timestamp and current logical timestamp to the events in the `events` table.

```mzsql
SELECT now(), mz_now(), * FROM events
```

```nofmt
            now            |    mz_now     | content |       event_ts
---------------------------+---------------+---------+-------------------------
 2023-06-15 22:38:14.18+00 | 1686868693480 | hello   | 2023-06-15 22:29:50.711
 2023-06-15 22:38:14.18+00 | 1686868693480 | goodbye | 2023-06-15 22:29:51.233
 2023-06-15 22:38:14.18+00 | 1686868693480 | welcome | 2023-06-15 22:29:50.874
(3 rows)
```

Notice when you try to materialize this query, you get errors:

```mzsql
CREATE MATERIALIZED VIEW cant_materialize
    AS SELECT now(), mz_now(), * FROM events;
```

```nofmt
ERROR:  cannot materialize call to current_timestamp
ERROR:  cannot materialize call to mz_now
```

[`mz_timestamp`]: /sql/types/mz_timestamp
[`timestamp with time zone`]: /sql/types/timestamptz


---

## Pushdown functions


`try_parse_monotonic_iso8601_timestamp` parses a subset of [ISO 8601]
timestamps that matches the 24 character length output
of the javascript [Date.toISOString()] function.
Unlike other parsing functions, inputs that fail to parse return `NULL`
instead of error.

This allows `try_parse_monotonic_iso8601_timestamp` to be used with
the [temporal filter pushdown] feature on `text` timestamps.
This is particularly useful when working with
[JSON sources](/sql/create-source/kafka/#format-json),
or other external data sources that store timestamps as strings.

Specifically, the accepted format is `YYYY-MM-DDThh:mm:ss.sssZ`:

- A 4-digit positive year, left-padded with zeros followed by
- A literal `-` followed by
- A 2-digit month, left-padded with zeros followed by
- A literal `-` followed by
- A 2-digit day, left-padded with zeros followed by
- A literal `T` followed by
- A 2-digit hour, left-padded with zeros followed by
- A literal `:` followed by
- A 2-digit minute, left-padded with zeros followed by
- A literal `:` followed by
- A 2-digit second, left-padded with zeros followed by
- A literal `.`
- A 3-digit millisecond, left-padded with zeros followed by
- A literal `Z`, indicating the UTC time zone.

Ordinary `text`-to-`timestamp` casts will prevent a filter from being pushed down.
Replacing those casts with `try_parse_monotonic_iso8601_timestamp` can unblock that
optimization for your query.

## Examples

```mzsql
SELECT try_parse_monotonic_iso8601_timestamp('2015-09-18T23:56:04.123Z') AS ts;
```
```nofmt
 ts
--------
 2015-09-18 23:56:04.123
```

 <hr/>

```mzsql
SELECT try_parse_monotonic_iso8601_timestamp('nope') AS ts;
```
```nofmt
 ts
--------
 NULL
```

[ISO 8601]: https://en.wikipedia.org/wiki/ISO_8601
[Date.toISOString()]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date/toISOString
[temporal filter pushdown]: /transform-data/patterns/temporal-filters/#temporal-filter-pushdown
[jsonb]: /sql/types/jsonb/


---

## string_agg function


The `string_agg(value, delimiter)` aggregate function concatenates the non-null
input values (i.e. `value`) into [`text`](/sql/types/text). Each value after the
first is preceded by its corresponding `delimiter`, where _null_ values are
equivalent to an empty string.
The input values to the aggregate can be [filtered](../filters).

## Syntax



```mzsql
string_agg ( <value>, <delimiter>
  [ORDER BY <col_ref> [ASC | DESC] [NULLS FIRST | NULLS LAST] [, ...]]
)
[FILTER (WHERE <filter_clause>)]

```

| Syntax element | Description |
| --- | --- |
| `<value>` | The values to concatenate.  |
| `<delimiter>` | The value to precede each concatenated value.  |
| **ORDER BY** `<col_ref>` [**ASC** \| **DESC**] [**NULLS FIRST** \| **NULLS LAST**] [, ...] | Optional. Specifies the ordering of values within the aggregation. If not specified, incoming rows are not guaranteed any order.  |
| **FILTER** (WHERE `<filter_clause>`) | Optional. Specifies which rows are sent to the aggregate function. Rows for which the `<filter_clause>` evaluates to true contribute to the aggregation. See [Aggregate function filters](/sql/functions/filters) for details.  |


## Signatures

Parameter | Type | Description
----------|------|------------
_value_    | `text`  | The values to concatenate.
_delimiter_  | `text`  | The value to precede each concatenated value.

### Return value

`string_agg` returns a [`text`](/sql/types/text) value.

This function always executes on the data from `value` as if it were sorted in ascending order before the function call. Any specified ordering is
ignored. If you need to perform aggregation in a specific order, you must specify `ORDER BY` within the aggregate function call itself. Otherwise incoming rows are not guaranteed any order.

### Usage in dataflows

While `string_agg` is available in Materialize, materializing views using it is
considered an incremental view maintenance anti-pattern. Any change to the data
underlying the function call will require the function to be recomputed
entirely, discarding the benefits of maintaining incremental updates.

Instead, we recommend that you materialize all components required for the
`string_agg` function call and create a non-materialized view using
`string_agg` on top of that. That pattern is illustrated in the following
statements:

```mzsql
CREATE MATERIALIZED VIEW foo_view AS SELECT * FROM foo;
CREATE VIEW bar AS SELECT string_agg(foo_view.bar, ',');
```

## Examples

```mzsql
SELECT string_agg(column1, column2)
FROM (
    VALUES ('z', ' !'), ('a', ' @'), ('m', ' #')
);
```
```nofmt
 string_agg
------------
 a #m !z
```

Note that in the following example, the `ORDER BY` of the subquery feeding into `string_agg` gets ignored.

```mzsql
SELECT column1, column2
FROM (
    VALUES ('z', ' !'), ('a', ' @'), ('m', ' #')
) ORDER BY column1 DESC;
```
```nofmt
 column1 | column2
---------+---------
 z       |  !
 m       |  #
 a       |  @
```

```mzsql
SELECT string_agg(column1, column2)
FROM (
    SELECT column1, column2
    FROM (
        VALUES ('z', ' !'), ('a', ' @'), ('m', ' #')
    ) f ORDER BY column1 DESC
) g;
```
```nofmt
 string_agg
------------
 a #m !z
```

```mzsql
SELECT string_agg(b, ',' ORDER BY a DESC) FROM table;
```


---

## SUBSTRING function


`SUBSTRING` returns a specified substring of a string.

## Signatures

```mzsql
substring(str, start_pos)
substring(str, start_pos, len)
substring(str FROM start_pos)
substring(str FOR len)
substring(str FROM start_pos FOR len)
```

Parameter | Type | Description
----------|------|------------
_str_ | [`string`](../../types/string) | The base string.
_start&lowbar;pos_ | [`int`](../../types/int) | The starting position for the substring; counting starts at 1.
_len_ | [`int`](../../types/int) | The length of the substring you want to return.

### Return value

`substring` returns a [`string`](../../types/string).

## Examples

```mzsql
SELECT substring('abcdefg', 3) AS substr;
```
```nofmt
 substr
--------
 cdefg
```

 <hr/>

```mzsql
SELECT substring('abcdefg', 3, 3) AS substr;
```
```nofmt
 substr
--------
 cde
```


---

## Table functions


## Overview

[Table functions](/sql/functions/#table-functions) return multiple rows from one
input row. They are typically used in the `FROM` clause, where their arguments
are allowed to refer to columns of earlier tables in the `FROM` clause.

For example, consider the following table whose rows consist of lists of
integers:

```mzsql
CREATE TABLE quizzes(scores int list);
INSERT INTO quizzes VALUES (LIST[5, 7, 8]), (LIST[3, 3]);
```

Query the `scores` column from the table:

```mzsql
SELECT scores
FROM quizzes;
```

The query returns two rows, where each row is a list:

```
 scores
---------
 {3,3}
 {5,7,8}
(2 rows)
```

Now, apply the [`unnest`](/sql/functions/#unnest) table function to expand the
`scores` list into a collection of rows, where each row contains one list item:

```mzsql
SELECT scores, score
FROM
  quizzes,
  unnest(scores) AS score; -- In Materialize, shorthand for AS t(score)
```

The query returns 5 rows, one row for each list item:

```
 scores  | score
---------+-------
 {3,3}   |     3
 {3,3}   |     3
 {5,7,8} |     5
 {5,7,8} |     7
 {5,7,8} |     8
(5 rows)
```

> **Tip:** For illustrative purposes, the original `scores` column is included in the
> results (i.e., query projection). In practice, you generally would omit
> including the original list to minimize the return data size.


## `WITH ORDINALITY`

When a table function is used in the `FROM` clause, you can add `WITH
ORDINALITY` after the table function call. `WITH ORDINALITY` adds a column that
includes the **1**-based numbering for each output row, restarting at **1** for
each input row.

The following example uses `unnest(...) WITH ORDINALITY` to include the `ordinality` column containing the **1**-based numbering of the unnested items:
```mzsql
SELECT scores, score, ordinality
FROM
  quizzes,
  unnest(scores) WITH ORDINALITY AS t(score,ordinality);
```

The results includes the `ordinality` column:
```
 scores  | score | ordinality
---------+-------+------------
 {3,3}   |     3 |          1
 {3,3}   |     3 |          2
 {5,7,8} |     5 |          1
 {5,7,8} |     7 |          2
 {5,7,8} |     8 |          3
(5 rows)
```

## Table- and column aliases

You can use table- and column aliases to name both the result column(s) of a table function as well as the ordinality column, if present. For example:
```mzsql
SELECT scores, t.score, t.listidx
FROM
  quizzes,
  unnest(scores) WITH ORDINALITY AS t(score,listidx);
```

You can also name fewer columns in the column alias list than the number of
columns in the output of the table function (plus `WITH ORDINALITY`, if
present), in which case the extra columns retain their original names.


## `ROWS FROM`

When you select from multiple relations without specifying a relationship, you
get a cross join. This is also the case when you select from multiple table
functions in `FROM` without specifying a relationship.

For example, consider the following query that selects from two table functions
without a relationship:

```mzsql
SELECT *
FROM
  generate_series(1, 2) AS g1,
  generate_series(6, 7) AS g2;
```

The query returns every combination of rows from both:

```

 g1 | g2
----+----
  1 |  6
  1 |  7
  2 |  6
  2 |  7
(4 rows)
```

Using `ROWS FROM` clause with the multiple table functions, you can zip the
outputs of the table functions (i.e., combine the n-th output row from each
table function into a single row) instead of the cross product.
That is, combine first output rows of all the table functions into the first row, the second output rows of all the table functions are combined into
a second row, and so on.

For example, modify the previous query to use `ROWS FROM` with the table
functions:

```mzsql
SELECT *
FROM
  ROWS FROM (
    generate_series(1, 2),
    generate_series(6, 7)
  ) AS t(g1, g2);
```

Instead of the cross product, the results are the "zipped" rows:

```
 g1 | g2
----+----
  1 |  6
  2 |  7
(2 rows)
```

If the table functions in a `ROWS FROM` clause produce a different number of
rows, nulls are used for padding:
```mzsql
SELECT *
FROM
  ROWS FROM (
    generate_series(1, 3),  -- 3 rows
    generate_series(6, 7)   -- 2 rows
  ) AS t(g1, g2);
```

The row with the `g1` value of 3 has a null `g2` value (note that if using psql,
psql prints null as an empty string):

```
| g1 | g2   |
| -- | ---- |
| 3  | null |
| 1  | 6    |
| 2  | 7    |
(3 rows)
```

For `ROWS FROM` clauses:
- you can use `WITH ORDINALITY` on the entire `ROWS FROM` clause, not on the
individual table functions within the `ROWS FROM` clause.
- you can use table- and column aliases only on the entire `ROWS FROM` clause,
not on the individual table functions within `ROWS FROM` clause.

For example:

```mzsql
SELECT *
FROM
  ROWS FROM (
    generate_series(5, 6),
    generate_series(8, 9)
  ) WITH ORDINALITY AS t(g1, g2, o);
```

The results contain the ordinality value in the `o` column:

```

 g1 | g2 | o
----+----+---
  5 |  8 | 1
  6 |  9 | 2
(2 rows)
```


## Table functions in the `SELECT` clause

You can call table functions in the `SELECT` clause. These will be executed as if they were at the end of the `FROM` clause, but their output columns will be at the appropriate position specified by their positions in the `SELECT` clause.

However, table functions in a `SELECT` clause have a number of restrictions (similar to Postgres):
- If there are multiple table functions in the `SELECT` clause, they are executed as if in an implicit `ROWS FROM` clause.
- `WITH ORDINALITY` and (explicit) `ROWS FROM` are not allowed.
- You can give a table function call a column alias, but not a table alias.
- If there are multiple output columns of a table function (e.g., `regexp_extract` has an output column per capture group), these will be combined into a single column, with a record type.

## Tabletized scalar functions

You can also call ordinary scalar functions in the `FROM` clause as if they were table functions. In that case, their output will be considered a table with a single row and column.

## See also

See a list of table functions in the [function reference](/sql/functions/#table-functions).


---

## TIMEZONE and AT TIME ZONE functions


`TIMEZONE` and `AT TIME ZONE` convert a [`timestamp`](../../types/timestamp/#timestamp-info) or a [`timestamptz`](../../types/timestamp/#timestamp-with-time-zone-info) to a different time zone.

**Known limitation:** You must explicitly cast the type for the time zone.

## Signatures



```mzsql
TIMEZONE ( <zone>::<type>, <timestamp> | <timestamptz> )

```

| Syntax element | Description |
| --- | --- |
| `<zone>` | A [`text`](/sql/types/text/) value specifying the target time zone.  |
| `<type>` | A [`text`](/sql/types/text/) or [`numeric`](/sql/types/numeric/) type specifying the datatype in which the time zone is expressed. **Known limitation:** You must explicitly cast the type for the time zone.  |
| `<timestamp>` | A [`timestamp`](/sql/types/timestamp/) value (timestamp without time zone).  |
| `<timestamptz>` | A [`timestamptz`](/sql/types/timestamp/) value (timestamp with time zone).  |




```mzsql
<timestamp> | <timestamptz> AT TIME ZONE <zone>::<type>

```

| Syntax element | Description |
| --- | --- |
| `<timestamp>` | A [`timestamp`](/sql/types/timestamp/) value (timestamp without time zone).  |
| `<timestamptz>` | A [`timestamptz`](/sql/types/timestamp/) value (timestamp with time zone).  |
| **AT TIME ZONE** `<zone>::<type>` | The target time zone. `<zone>` is a [`text`](/sql/types/text/) value. `<type>` is a [`text`](/sql/types/text/) or [`numeric`](/sql/types/numeric/) type. **Known limitation:** You must explicitly cast the type for the time zone.  |


Parameter | Type | Description
----------|------|------------
_zone_ | [`text`](../../types/text) | The target time zone.
_type_  |[`text`](../../types/text) or [`numeric`](../../types/numeric) |  The datatype in which the time zone is expressed
_timestamp_ | [`timestamp`](../../types/timestamp/#timestamp-info) | The timestamp without time zone.  |   |
_timestamptz_ | [`timestamptz`](../../types/timestamp/#timestamp-with-time-zone-info) | The timestamp with time zone.

## Return values

`TIMEZONE` and  `AT TIME ZONE` return [`timestamp`](../../types/timestamp/#timestamp-info) if the input is [`timestamptz`](../../types/timestamp/#timestamp-with-time-zone-info), and [`timestamptz`](../../types/timestamp/#timestamp-with-time-zone-info) if the input is [`timestamp`](../../types/timestamp/#timestamp-info).

**Note:** `timestamp` and `timestamptz` always store data in UTC, even if the date is returned as the local time.

## Examples

### Convert timestamp to another time zone, returned as UTC with offset

```mzsql
SELECT TIMESTAMP '2020-12-21 18:53:49' AT TIME ZONE 'America/New_York'::text;
```
```
        timezone
------------------------
2020-12-21 23:53:49+00
(1 row)
```

```mzsql
SELECT TIMEZONE('America/New_York'::text,'2020-12-21 18:53:49');
```
```
        timezone
------------------------
2020-12-21 23:53:49+00
(1 row)
```

### Convert timestamp to another time zone, returned as specified local time

```mzsql
SELECT TIMESTAMPTZ '2020-12-21 18:53:49+08' AT TIME ZONE 'America/New_York'::text;
```
```
        timezone
------------------------
2020-12-21 05:53:49
(1 row)
```

```mzsql
SELECT TIMEZONE ('America/New_York'::text,'2020-12-21 18:53:49+08');
```
```
        timezone
------------------------
2020-12-21 05:53:49
(1 row)
```

## Related topics
* [`timestamp` and `timestamp with time zone` data types](../../types/timestamp)


---

## to_char function


`to_char` converts a timestamp into a string using the specified format.

The format string can be composed of any number of [format
specifiers](#format-specifiers), interspersed with regular text. You can place a
specifier token inside of double-quotes to emit it literally.

## Examples

#### RFC 2822 format

```mzsql
SELECT to_char(TIMESTAMPTZ '2019-11-26 15:56:46 +00:00', 'Dy, Mon DD YYYY HH24:MI:SS +0000') AS formatted
```
```nofmt
             formatted
 ---------------------------------
  Tue, Nov 26 2019 15:56:46 +0000
```

#### Additional non-interpreted text

Normally the `W` in "Welcome" would be converted to the week number, so we must quote it.
The "to" doesn't match any format specifiers, so quotes are optional.

```mzsql
SELECT to_char(TIMESTAMPTZ '2019-11-26 15:56:46 +00:00', '"Welcome" to Mon, YYYY') AS formatted
```
```nofmt
       formatted
 ----------------------
  Welcome to Nov, 2019
```

#### Ordinal modifiers

```mzsql
SELECT to_char(TIMESTAMPTZ '2019-11-1 15:56:46 +00:00', 'Dth of Mon') AS formatted
```
```nofmt
  formatted
 ------------
  6th of Nov
```

## Format specifiers

| Specifier     | Description                                                                                      |
|---------------|--------------------------------------------------------------------------------------------------|
| `HH`          | hour of day (01-12)                                                                              |
| `HH12`        | hour of day (01-12)                                                                              |
| `HH24`        | hour of day (00-23)                                                                              |
| `MI`          | minute (00-59)                                                                                   |
| `SS`          | second (00-59)                                                                                   |
| `MS`          | millisecond (000-999)                                                                            |
| `US`          | microsecond (000000-999999)                                                                      |
| `SSSS`        | seconds past midnight (0-86399)                                                                  |
| `AM`/`PM`     | uppercase meridiem indicator (without periods)                                                   |
| `am`/`pm`     | lowercase meridiem indicator (without periods)                                                   |
| `A.M.`/`P.M.` | uppercase meridiem indicator (with periods)                                                      |
| `a.m.`/`p.m.` | lowercase meridiem indicator (with periods)                                                      |
| `Y,YYY`       | Y,YYY year (4 or more digits) with comma                                                         |
| `YYYY`        | year (4 or more digits)                                                                          |
| `YYY`         | last 3 digits of year                                                                            |
| `YY`          | last 2 digits of year                                                                            |
| `Y`           | last digit of year                                                                               |
| `IYYY`        | ISO 8601 week-numbering year (4 or more digits)                                                  |
| `IYY`         | last 3 digits of ISO 8601 week-numbering year                                                    |
| `IY`          | last 2 digits of ISO 8601 week-numbering year                                                    |
| `I`           | last digit of ISO 8601 week-numbering year                                                       |
| `BC`/`AD`     | uppercase era indicator (without periods)                                                        |
| `bc`/`ad`     | lowercase era indicator (without periods)                                                        |
| `B.C.`/`A.D.` | uppercase era indicator (with periods)                                                           |
| `b.c.`/`a.d.` | lowercase era indicator (with periods)                                                           |
| `MONTH`       | full upper case month name (blank-padded to 9 chars)                                             |
| `Month`       | full capitalized month name (blank-padded to 9 chars)                                            |
| `month`       | full lower case month name (blank-padded to 9 chars)                                             |
| `MON`         | abbreviated upper case month name (3 chars in English, localized lengths vary)                   |
| `Mon`         | abbreviated capitalized month name (3 chars in English, localized lengths vary)                  |
| `mon`         | abbreviated lower case month name (3 chars in English, localized lengths vary)                   |
| `MM`          | month number (01-12)                                                                             |
| `DAY`         | full upper case day name (blank-padded to 9 chars)                                               |
| `Day`         | full capitalized day name (blank-padded to 9 chars)                                              |
| `day`         | full lower case day name (blank-padded to 9 chars)                                               |
| `DY`          | abbreviated upper case day name (3 chars in English, localized lengths vary)                     |
| `Dy`          | abbreviated capitalized day name (3 chars in English, localized lengths vary)                    |
| `dy`          | abbreviated lower case day name (3 chars in English, localized lengths vary)                     |
| `DDD`         | day of year (001-366)                                                                            |
| `IDDD`        | day of ISO 8601 week-numbering year (001-371; day 1 of the year is Monday of the first ISO week) |
| `DD`          | day of month (01-31)                                                                             |
| `D`           | day of the week, Sunday (1) to Saturday (7)                                                      |
| `ID`          | ISO 8601 day of the week, Monday (1) to Sunday (7)                                               |
| `W`           | week of month (1-5) (the first week starts on the first day of the month)                        |
| `WW`          | week number of year (1-53) (the first week starts on the first day of the year)                  |
| `IW`          | week number of ISO 8601 week-numbering year (01-53; the first Thursday of the year is in week 1) |
| `CC`          | century (2 digits) (the twenty-first century starts on 2001-01-01)                               |
| `J`           | Julian Day (days since November 24, 4714 BC at midnight)                                         |
| `Q`           | quarter (ignored by to_date and to_timestamp)                                                    |
| `RM`          | month in upper case Roman numerals (I-XII; I=January)                                            |
| `rm`          | month in lower case Roman numerals (i-xii; i=January)                                            |
| `TZ`          | upper case time-zone name                                                                        |
| `tz`          | lower case time-zone name                                                                        |

### Specifier modifiers

| Modifier         | Description                                            | Example | Without Modification | With Modification |
|------------------|--------------------------------------------------------|---------|----------------------|-------------------|
| `FM` prefix      | fill mode (suppress leading zeroes and padding blanks) | FMMonth | `July    `           | `July`            |
| `TH`/`th` suffix | upper/lower case ordinal number suffix                 | Dth     | `1`                  | `1st`             |
