# Slim deployments

How to use dbt for slim deployments.



> **Tip:** Once your dbt project is ready to move out of development, or as soon as you
> start managing multiple users and deployment environments, we recommend
> checking the code in to **version control** and setting up an **automated
> workflow** to control the deployment of changes.
>


[//]: # "TODO(morsapaes) Consider moving demos to template repo."

On each run, dbt generates [artifacts](https://docs.getdbt.com/reference/artifacts/dbt-artifacts)
with metadata about your dbt project, including the [_manifest file_](https://docs.getdbt.com/reference/artifacts/manifest-json)
(`manifest.json`). This file contains a complete representation of the latest
state of your project, and you can use it to **avoid re-deploying resources
that didn't change** since the last run.

We recommend using the slim deployment pattern when you want to reduce
development idle time and CI costs in development environments. For
production deployments, you should prefer the [blue/green deployment pattern](/manage/dbt/blue-green-deployments/).


> **Note:** Check [this demo](https://github.com/morsapaes/dbt-ci-templates) for a sample
> end-to-end workflow using GitHub and GitHub Actions.
>


1. Fetch the production `manifest.json` file into the CI environment:

    ```bash
          - name: Download production manifest from s3
            env:
              AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
              AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
              AWS_SESSION_TOKEN: ${{ secrets.AWS_SESSION_TOKEN }}
              AWS_REGION: us-east-1
            run: |
              aws s3 cp s3://mz-test-dbt/manifest.json ./manifest.json
    ```

1. Then, instruct dbt to run and test changed models and dependencies only:

    ```bash
          - name: Build dbt
            env:
              MZ_HOST: ${{ secrets.MZ_HOST }}
              MZ_USER: ${{ secrets.MZ_USER }}
              MZ_PASSWORD: ${{ secrets.MZ_PASSWORD }}
              CI_TAG: "${{ format('{0}_{1}', 'gh_ci', github.event.number ) }}"
            run: |
              source .venv/bin/activate
              dbt run-operation drop_environment
              dbt build --profiles-dir ./ --select state:modified+ --state ./ --target production
    ```

    In the example above, `--select state:modified+` instructs dbt to run all
    models that were modified (`state:modified`) and their downstream
    dependencies (`+`). Depending on your deployment requirements, you might
    want to use a different combination of state selectors, or go a step
    further and use the [`--defer`](https://docs.getdbt.com/reference/node-selection/defer)
    flag to reduce even more the number of models that need to be rebuilt.
    For a full rundown of the available [state modifier](https://docs.getdbt.com/reference/node-selection/methods#the-state-method)
    and [graph operator](https://docs.getdbt.com/reference/node-selection/graph-operators)
    options, check the [dbt documentation](https://docs.getdbt.com/reference/node-selection/syntax).

1. Every time you deploy to production, upload the new `manifest.json` file to
   blob storage (e.g. s3):

    ```bash
          - name: upload new manifest to s3
            env:
              AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
              AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
              AWS_SESSION_TOKEN: ${{ secrets.AWS_SESSION_TOKEN }}
              AWS_REGION: us-east-1
            run: |
              aws s3 cp ./target/manifest.json s3://mz-test-dbt
    ```
