# ALTER INDEX

`ALTER INDEX` changes the parameters of an index.



Use `ALTER INDEX` to:
- Rename an index.

## Syntax


**Rename:**

### Rename

To rename an index:



```mzsql
ALTER INDEX <name> RENAME TO <new_name>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The current name of the index you want to alter.  |
| `<new_name>` | The new name of the index.  |
See also [Renaming restrictions](/sql/identifiers/#renaming-restrictions).







## Privileges

The privileges required to execute this statement are:

<ul>
<li>Ownership of the index.</li>
</ul>


## Related pages

- [`SHOW INDEXES`](/sql/show-indexes)
- [`SHOW CREATE VIEW`](/sql/show-create-view)
- [`SHOW VIEWS`](/sql/show-views)
- [`SHOW SOURCES`](/sql/show-sources)
- [`SHOW SINKS`](/sql/show-sinks)
