# DROP SOURCE

`DROP SOURCE` removes a source from Materialize.



`DROP SOURCE` removes a source from Materialize. If there are objects depending
on the source, you must explicitly drop them first, or use the `CASCADE`
option.

## Syntax

```mzsql
DROP SOURCE [IF EXISTS] <source_name> [RESTRICT|CASCADE];
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the named source does not exist.
`<source_name>` | The name of the source you want to remove.
**CASCADE** | Optional. If specified, remove the source and its dependent objects.
**RESTRICT** | Optional. Do not remove this source if it has dependent objects. _(Default.)_

## Examples

### Remove a source with no dependent objects

```mzsql
SHOW SOURCES;
```
```nofmt
...
my_source
```
```mzsql
DROP SOURCE my_source;
```

### Remove a source with dependent objects

```mzsql
SHOW SOURCES;
```
```nofmt
...
my_source
```
```mzsql
DROP SOURCE my_source CASCADE;
```

### Remove a source only if it has no dependent objects

You can use either of the following commands:

- ```mzsql
  DROP SOURCE my_source;
  ```
- ```mzsql
  DROP SOURCE my_source RESTRICT;
  ```

### Do not issue an error if attempting to remove a nonexistent source

```mzsql
DROP SOURCE IF EXISTS my_source;
```

## Privileges

The privileges required to execute this statement are:

<ul>
<li>Ownership of the dropped source.</li>
<li><code>USAGE</code> privileges on the containing schema.</li>
</ul>


## Related pages

- [`CREATE SOURCE`](../create-source)
- [`SHOW SOURCES`](../show-sources)
- [`SHOW CREATE SOURCE`](../show-create-source)
- [`DROP OWNED`](../drop-owned)
