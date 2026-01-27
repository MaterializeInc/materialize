# jsonb type
Expresses a JSON object
`jsonb` data expresses a JSON object similar to
[PostgreSQL's implementation](https://www.postgresql.org/docs/current/datatype-json.html).

Detail | Info
-------|------
**Quick Syntax** | `'{"1":2,"3":4}'::jsonb`
**Size** | Variable
**Catalog name** | `pg_catalog.jsonb`
**OID** | 3802

Materialize does not yet support a type more similar to PostgreSQL's
implementation of `json`.

## Syntax



```mzsql
'<json_string>'::JSONB

```

| Syntax element | Description |
| --- | --- |
| `'<json_string>'` | A well-formed [JSON object](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/JSON) string.  |


## `jsonb` functions + operators

Materialize supports the following operators and functions.

### Operators

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


### Functions

#### `jsonb_agg(expression) -> jsonb`

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

<code>v</code> as <code>jsonb</code> [(docs)](/sql/types/jsonb#to_jsonb)

#### Detail

Functions that return `Col`s are considered table functions and can only be used
as tables, i.e. you cannot use them as scalar values. For example, you can only
use `jsonb_object_keys` in the following way:

```mzsql
SELECT * FROM jsonb_object_keys('{"1":2,"3":4}'::jsonb);
```

## Details

- `jsonb` elements can be of the following types:
  - Objects
  - Arrays
  - String
  - Number
  - Boolean
  - Null
- Numbers in `jsonb` elements are all equivalent to
  [`numeric`](/sql/types/numeric) in SQL.

### Valid casts

#### From `jsonb`

You can [cast](../../functions/cast) `jsonb` to:

- [`boolean`](../boolean) (explicitly)
- [`numeric`](../numeric) (explicitly)
- [`int`](../integer) (explicitly)
- [`real`/`double precision`](../float) (explicitly)
- [`text`](../text) (by assignment) (stringifies `jsonb`)

#### To `jsonb`

You can explicitly [cast](../../functions/cast) from [`text`](../text) to `jsonb`.

#### Notes about converting `jsonb` to `text`

`jsonb` can have some odd-feeling corner cases when converting to or from
[`text`](/sql/types/text).

- `jsonb::text` always produces the printed version of the JSON.

    ```mzsql
    SELECT ('"a"'::jsonb)::text AS jsonb_elem;
    ```
    ```nofmt
     jsonb_elem
    ------------
     "a"
    ```

- `->>` and the `_text` functions produce the printed version of the inner
  element, unless the output is a single JSON string in which case they print it
  without quotes, i.e. as a SQL `text` value.

    ```mzsql
    SELECT ('"a"'::jsonb)->>0 AS string_elem;
    ```
    ```nofmt
     jsonb_elem
    ------------
     a
    ```

- `text` values passed to `to_jsonb` with quotes (`"`) produced `jsonb` strings
  with the quotes escaped.

    ```mzsql
    SELECT to_jsonb('"foo"') AS escaped_quotes;
    ```
    ```nofmt
     escaped_quotes
    ----------------
     "\"foo\""
    ```

### Subscripting

You can use subscript notation (`[]`) to extract an element from a `jsonb` array
or object.

The returned value is always of type `jsonb`. If the requested array element or
object key does not exist, or if either the input value or subscript value is
`NULL`, the subscript operation returns `NULL`.

#### Arrays

To extract an element from an array, supply the 0-indexed position as the
subscript:

```mzsql
SELECT ('[1, 2, 3]'::jsonb)[1]
```
```nofmt
 jsonb
-------
 2
```

Negative indexes count backwards from the end of the array. [Slice syntax] is
not supported. Note also that 0-indexed positions are at variance with [`list`]
and [`array`] types, whose subscripting operation uses 1-indexed positions.

#### Objects

To extract a value from an object, supply the key as the subscript:

```mzsql
SELECT ('{"a": 1, "b": 2, "c": 3}'::jsonb)['b'];
```
```nofmt
 jsonb
-------
 2
```

You can chain subscript operations to retrieve deeply nested elements:

```mzsql
SELECT ('{"1": 2, "a": ["b", "c"]}'::jsonb)['a'][1];
```
```nofmt
 jsonb
-------
 "c"
```

#### Remarks

Because the output type of the subscript operation is always `jsonb`, when
comparing the output of a subscript to a string, you must supply a JSON string
to compare against:

```mzsql
SELECT ('["a", "b"]::jsonb)[1] = '"b"'
```

Note the extra double quotes on the right-hand side of the comparison.

### Parsing

Manually parsing JSON-formatted data in SQL can be tedious. You can use the [interactive JSON parser widget](https://materialize.com/docs/sql/types/jsonb/#parsing) to automatically turn a sample JSON payload into a parsing view with the individual fields mapped to columns.


## Examples

### Operators

#### Field access as `jsonb` (`->`)

The type of JSON element you're accessing dictates the RHS's type.

- Use a `string` to return the value for a specific key:

  ```mzsql
  SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb->'1' AS field_jsonb;
  ```
  ```nofmt
   field_jsonb
  -------------
   2
  ```

- Use an `int` to return the value in an array at a specific index:

  ```mzsql
  SELECT '["1", "a", 2]'::jsonb->1 AS field_jsonb;
  ```
  ```nofmt
   field_jsonb
  -------------
   "a"
  ```
Field accessors can also be chained together.

```mzsql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb->'a'->1 AS field_jsonb;
```
```nofmt
 field_jsonb
-------------
 "c"
```

Note that all returned values are `jsonb`.

<hr/>

#### Field access as `text` (`->>`)

The type of JSON element you're accessing dictates the RHS's type.

- Use a `string` to return the value for a specific key:

  ```mzsql
  SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb->>'1' AS field_text;
  ```
  ```nofmt
   field_text
  -------------
   2
  ```

- Use an `int` to return the value in an array at a specific index:

  ```mzsql
  SELECT '["1", "a", 2]'::jsonb->>1 AS field_text;
  ```
  ```nofmt
   field_text
  -------------
   a
  ```

Field accessors can also be chained together, as long as the LHS remains
`jsonb`.

```mzsql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb->'a'->>1 AS field_text;
```
```nofmt
 field_text
-------------
 c
```

Note that all returned values are `string`.

#### Path access as `jsonb` (`#>`)

You can access specific elements in a `jsonb` value using a "path", which is a
[text array](/sql/types/array) where each element is either a field key or an
array element:

```mzsql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb #> '{a,1}' AS field_jsonb;
```
```nofmt
 field_jsonb
-------------
 "c"
```

The operator returns a value of type `jsonb`. If the path is invalid, it returns
`NULL`.

#### Path access as `text` (`#>>`)

The `#>>` operator is equivalent to the [`#>`](#path-access-as-jsonb-) operator,
except that the operator returns a value of type `text`.

```mzsql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb #>> '{a,1}' AS field_text;
```
```nofmt
 field_text
-------------
 c
```

<hr/>

#### `jsonb` concat (`||`)

```mzsql
SELECT '{"1": 2}'::jsonb ||
       '{"a": ["b", "c"]}'::jsonb AS concat;
```
```nofmt
             concat
---------------------------------
 {"1":2,"a":["b","c"]}
```

<hr/>

#### Remove key (`-`)

```mzsql
 SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb - 'a' AS rm_key;
```
```nofmt
  rm_key
-----------
 {"1":2}
```

<hr/>

#### LHS contains RHS (`@>`)

Here, the left hand side does contain the right hand side, so the result is `t` for true.

```mzsql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb @>
       '{"1": 2}'::jsonb AS lhs_contains_rhs;
```
```nofmt
 lhs_contains_rhs
------------------
 t
```

<hr/>

#### RHS contains LHS (`<@`)

Here, the right hand side does contain the left hand side, so the result is `t` for true.

```mzsql
SELECT '{"1": 2}'::jsonb <@
       '{"1": 2, "a": ["b", "c"]}'::jsonb AS lhs_contains_rhs;
```
```nofmt
 rhs_contains_lhs
------------------
 t
```

<hr/>

#### Search top-level keys (`?`)

```mzsql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb ? 'a' AS search_for_key;
```
```nofmt
 search_for_key
----------------
 t
```

```mzsql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb ? 'b' AS search_for_key;
```
```nofmt
 search_for_key
----------------
 f
```

### Functions

#### `jsonb_array_elements`

##### Expanding a JSON array

```mzsql
SELECT * FROM jsonb_array_elements('[true, 1, "a", {"b": 2}, null]'::jsonb);
```
```nofmt
   value
-----------
 true
 1.0
 "a"
 {"b":2.0}
 null
```

##### Flattening a JSON array

```mzsql
SELECT t.id,
       obj->>'a' AS a,
       obj->>'b' AS b
FROM (
  VALUES
    (1, '[{"a":1,"b":2},{"a":3,"b":4}]'::jsonb),
    (2, '[{"a":5,"b":6},{"a":7,"b":8}]'::jsonb)
) AS t(id, json_col)
CROSS JOIN jsonb_array_elements(t.json_col) AS obj;
```

```nofmt
 id | a | b
----+---+---
  1 | 1 | 2
  1 | 3 | 4
  2 | 5 | 6
  2 | 7 | 8
```

<hr/>


#### `jsonb_array_elements_text`

```mzsql
SELECT * FROM jsonb_array_elements_text('[true, 1, "a", {"b": 2}, null]'::jsonb);
```
```nofmt
   value
-----------
 true
 1.0
 "a"
 {"b":2.0}
 null
```

<hr/>

#### `jsonb_array_length`

```mzsql
SELECT jsonb_array_length('[true, 1, "a", {"b": 2}, null]'::jsonb);
```
```nofmt
 jsonb_array_length
--------------------
                  5
```

<hr/>

#### `jsonb_build_array`

```mzsql
SELECT jsonb_build_array('a', 1::float, 2.0::float, true);
```
```nofmt
 jsonb_build_array
--------------------
 ["a",1.0,2.0,true]
```

<hr/>

#### `jsonb_build_object`

```mzsql
SELECT jsonb_build_object(2.0::float, 'b', 'a', 1.1::float);
```
```nofmt
 jsonb_build_object
--------------------
 {"2":"b","a":1.1}
```

<hr/>

#### `jsonb_each`

```mzsql
SELECT * FROM jsonb_each('{"1": 2.1, "a": ["b", "c"]}'::jsonb);
```
```nofmt
 key |   value
-----+-----------
 1   | 2.1
 a   | ["b","c"]
```

Note that the `value` column is `jsonb`.

<hr/>

#### `jsonb_each_text`

```mzsql
SELECT * FROM jsonb_each_text('{"1": 2.1, "a": ["b", "c"]}'::jsonb);
```
```nofmt
 key |   value
-----+-----------
 1   | 2.1
 a   | ["b","c"]
```

Note that the `value` column is `string`.

<hr/>

#### `jsonb_object_keys`

```mzsql
SELECT * FROM jsonb_object_keys('{"1": 2, "a": ["b", "c"]}'::jsonb);
```
```nofmt
 jsonb_object_keys
-------------------
 1
 a
```

<hr/>

#### `jsonb_pretty`

```mzsql
SELECT jsonb_pretty('{"1": 2, "a": ["b", "c"]}'::jsonb);
```
```nofmt
 jsonb_pretty
--------------
 {           +
   "1": 2,   +
   "a": [    +
     "b",    +
     "c"     +
   ]         +
 }
```

<hr/>

#### `jsonb_typeof`

```mzsql
SELECT jsonb_typeof('[true, 1, "a", {"b": 2}, null]'::jsonb);
```
```nofmt
 jsonb_typeof
--------------
 array
```

```mzsql
SELECT * FROM jsonb_typeof('{"1": 2, "a": ["b", "c"]}'::jsonb);
```
```nofmt
 jsonb_typeof
--------------
 object
```

<hr/>

#### `jsonb_strip_nulls`

```mzsql
SELECT jsonb_strip_nulls('[{"1":"a","2":null},"b",null,"c"]'::jsonb);
```
```nofmt
    jsonb_strip_nulls
--------------------------
 [{"1":"a"},"b",null,"c"]
```

<hr/>

#### `to_jsonb`

```mzsql
SELECT to_jsonb(t) AS jsonified_row
FROM (
  VALUES
  (1, 'hey'),
  (2, NULL),
  (3, 'hi'),
  (4, 'salutations')
  ) AS t(id, content)
WHERE t.content LIKE 'h%';
```
```nofmt
      jsonified_row
--------------------------
 {"content":"hi","id":3}
 {"content":"hey","id":1}
```

Note that the output is `jsonb`.

[Slice syntax]: /sql/types/list#slicing-ranges
[`list`]: /sql/types/list
[`array`]: /sql/types/array
