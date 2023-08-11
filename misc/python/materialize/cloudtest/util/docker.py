import os
import uuid
from typing import Any, Dict

from .jwt import JWK_PUBLIC_KEY


def docker_env() -> Dict[Any, str]:
    docker_env = os.environ.copy()
    docker_env.update(
        FRONTEGG_JWK=JWK_PUBLIC_KEY.decode("utf8"),
        FRONTEGG_URL="https://cloud.materialize.com",
        # Since these tests don't run against a valid Frontegg workspace, bypass account blocking checks
        MZCLOUD_SYNC_SERVER_FRONTEGG_DISABLED="true",
        FRONTEGG_CLIENT_ID=str(uuid.uuid4()),
        FRONTEGG_SECRET_KEY=str(uuid.uuid4()),
        # End account blocking bypass block
        ENVIRONMENTD_IAM_ROLE_ARN="arn:aws:ec2:us-east-1:123445667:iam-role/environmentd-11223344551122334",
        IAM_ROLE_ARN="arn:aws:ec2:us-east-1:123445667:iam-role/controller-11223344551122334",
        STACK_TYPE="kind",
        CLOUD_PROVIDER="aws",
    )
    return docker_env
