# ALTER VIEW

`ALTER VIEW` changes properties of a view.



Use `ALTER VIEW` to:
- Rename a view.
- Change owner of a view.

## Syntax


**Rename:**

### Rename

To rename a view:



```mzsql
ALTER VIEW <name> RENAME TO <new_name>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The current name of the view.  |
| `<new_name>` | The new name of the view.  |
See also [Renaming restrictions](/sql/identifiers/#renaming-restrictions).



**Change owner:**

### Change owner

To change the owner of a view:



```mzsql
ALTER VIEW <name> OWNER TO <new_owner_role>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the view you want to change ownership of.  |
| `<new_owner_role>` | The new owner of the view.  |
To change the owner of a view, you must be the current owner and have
membership in the `<new_owner_role>`.






## Privileges

The privileges required to execute this statement are:

<ul>
<li>Ownership of the view being altered.</li>
<li>In addition, to change owners:
<ul>
<li>Role membership in <code>new_owner</code>.</li>
<li><code>CREATE</code> privileges on the containing schema if the view is namespaced by
a schema.</li>
</ul>
</li>
</ul>
