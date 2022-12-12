# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from datetime import datetime, timedelta
from os import environ

import launchdarkly_api  # type: ignore
from launchdarkly_api.api import feature_flags_api  # type: ignore

MAX_AGE = timedelta(hours=24)

# Access keys required for interacting with LaunchDarkly.
LAUNCHDARKLY_API_TOKEN = environ.get("LAUNCHDARKLY_API_TOKEN")


def clean_up_test_features() -> None:
    print(f"Deleting LaunchDarkly features whose age exceeds {MAX_AGE}")

    configuration = launchdarkly_api.Configuration(
        api_key=dict(ApiKey=LAUNCHDARKLY_API_TOKEN)
    )
    with launchdarkly_api.ApiClient(configuration) as api_client:
        api = feature_flags_api.FeatureFlagsApi(api_client)

        project_key = "default"
        now = datetime.utcnow()

        flags = api.get_feature_flags(
            project_key,
            env="ci-cd",
            tag="ci-test",
            archived=True,
        )

        for flag in flags["items"]:
            key = flag["key"]
            age = now - datetime.fromtimestamp(flag["creation_date"] / 1000)
            if age <= MAX_AGE:
                print(f"Skipping flag {key} (age={age}) whose age is beneath threshold")
            else:
                try:
                    print(f"Deleting flag {key} (age={age})")
                    api.delete_feature_flag(project_key, key)
                except Exception as e:
                    print(f"Error while trying to delete flag {key}: {e}")


def main() -> None:
    clean_up_test_features()


if __name__ == "__main__":
    main()
