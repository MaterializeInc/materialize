# CREATE SECRET

`CREATE SECRET` securely stores credentials in Materialize's secret management system.



A secret securely stores sensitive credentials (like passwords and SSL keys) in Materialize's secret management system. Optionally, a secret can also be used to store credentials that are generally not sensitive (like usernames and SSL certificates), so that all your credentials are managed uniformly.

## Syntax



```mzsql
CREATE SECRET [IF NOT EXISTS] <name> AS <value>;

```

| Syntax element | Description |
| --- | --- |
| `IF NOT EXISTS` | If specified, do not generate an error if a secret of the same name already exists.  |
| `<name>` | The identifier for the secret.  |
| `<value>` | The value for the secret. The value expression may not reference any relations, and must be implicitly castable to `bytea`.  |


## Examples

```mzsql
CREATE SECRET kafka_ca_cert AS decode('c2VjcmV0Cg==', 'base64');
```

## Privileges

The privileges required to execute this statement are:

<ul>
<li><code>CREATE</code> privileges on the containing schema.</li>
</ul>


## Related pages

- [`CREATE CONNECTION`](../create-connection)
- [`ALTER SECRET`](../alter-secret)
- [`DROP SECRET`](../drop-secret)
- [`SHOW SECRETS`](../show-secrets)
