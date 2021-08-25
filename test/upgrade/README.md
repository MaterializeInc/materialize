
# testdrive-based upgrade test framework

This upgrade test framework serves to verify that objects created in a previous version of materialize are still operational after an upgrade.

## Mode of operation

The framework does the following:
- fires an old version of Materialize
- runs the applicable 'create-in' .td tests against it
- kills the old version and starts a Materialize binary from your branch, preserving the mzdata directory across restarts
- runs the applicable 'check-from' .td tests

Kafka and the Schema Registry are not restarted.

A separate sub-workflow also tests the "upgrade" from your branch back to your branch.

## Test naming convention

Since different versions support different functionality, the framework makes sure that objects are only created in versions that support them and any tests against those objects are only run if the objects were created in first place.

This is achieved by following a test naming convention for the .td files:

```(create-in|check-from)-(version_name)-(test_name).td```

Tests for version_name = X will be run when testing upgrade from version X and when testing upgrade from any future version. Version names are specified in the format used to tag the containers:

- v0.6.1
- v0.7.3
- v0.8.0

There are also two other special version identifiers:

- ```current_source``` will only run when testing the "upgrade" from your current source to your current source
- ```any_version``` tests will run when upgrading from any version

## Adding a new Materialize version to the test

1. Add a step under ```upgrade``` in ```mzcompose.yml```

  ```
  upgrade:
    steps:
      - step: workflow
        workflow: upgrade-from-0_6_1
  ```
2. Add a section:

```
  upgrade-from-0_8_2:
    env:
      UPGRADE_FROM_VERSION: v0.8.2
      TESTS: any_version|v0.6.1|v0.7.3|v0.8.0|v0.8.1|v0.8.2
    steps:
      - step: workflow
        workflow: test-upgrade-from-version
```

Make sure the ```TESTS``` line includes the version under test **and all other** versions prior to it that are already in the file.

## Adding a new test

### For an existing feature

1. Decide which is the earlest version ```vX.Y.Z``` that supports the desired functionality and create a test named ```create-in-vX.Y.Z-feature_under_test.td``` where you will be creating the database objects that will be surviving an upgrade attempt. Use ```any_version``` if the feature exists in all versions listed in ```mzcompose.yml``` and `current_source` if you are adding the feature just now and it does not exist in any previously released version.

2. In a file named ```check-from-vX.Y.Z-feature_undex_test.td``` put the queries that will be testing that the object has survived the upgrade intact. This may include any of the following:

- checking that the metadata has survived intact both in the catalog and as observed by an end user, e.g.  ```pg_typeof```
- checking that the object can be ```SELECT```ed from and returns the right data
- if a source, check that new data can still be ingested
- if a sink, check that new data can still be output post-upgrade
- if any DDL statements can be run against the object, test them. At the very least, test that ```DROP``` is able to drop the object (and confirm that the object is gone).

### For a feature you are currently developing

Follow the instructions above, but use `current_source` as the version name within the name of the `.td` files. The `mkrelease.py` will rename
your test and replace `current_source` with the actual name of the version your feature has been released in.
