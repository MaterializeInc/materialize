# Mz Backwards Compatibility

Since the Console queries [system catalog tables](https://materialize.com/docs/sql/system-catalog/) in Materialize, there will be cases where the schema of a table will change. Since customer environments can upgrade at different times, we need to support both versions of Materialize until all environments are successfully upgraded. Here are a few ways to do this:

## useEnvironmentGate

We have a hook called `useEnvironmentGate` that returns true if the environment greater than or equal to the version you input. We can use it on runtime to handle both versions then create a follow up issue to cleanup when all environments are upgraded. To figure out which version to use, you can use the [Release Schedule](https://www.notion.so/materialize/d4b554c5a5f240339985872f04a6aa1e?v=1107e57b8c9342068c707b7693244326&pvs=4) notion document as the source of truth.

## Testing on different versions

If you need to test on different versions, you can create a test environment on Staging by signing up with your Materialize email but with a different alias. For example, for jun@materialize.com, you can sign up with jun+1@materialize.com. On this test environment, you can deploy a custom version to it by [following this guide](https://www.notion.so/materialize/Debugging-Materialize-Cloud-0788c1fe692b4acf9d247345e7b5346e#b2da86e74f814f94bdfcbf8cd423720a).

## Handling Kysely

We use Kysely to code generate type definitions for our system catalog tables. At the time of writing, these types are generated in `materialize.d.ts`. Since we can't use runtime checks to get around type errors, we can get around this by manually editing `materialize.d.ts` then creating a follow up issue to do another codegen to synchronize. There are a few cases that might happen:

1. New columns are added in the new version

We can add these columns to the appropriate tables

2. Columns are deleted in the new version

We can keep these columns around for the previous version given they shouldn't break any current behavior

3. The type of a column is changed in the new version

We can edit the type of a column to be a discriminated union then do runtime checks to handle the discriminated union.

Make sure to annotate the change with a TODO so that codegens from other PRs before cleanup don't dismiss it too easily.
