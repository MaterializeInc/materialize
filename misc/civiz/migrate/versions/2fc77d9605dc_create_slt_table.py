# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from alembic import op
import sqlalchemy as sa


revision = '2fc77d9605dc'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "slt",
        sa.Column("commit", sa.Text, primary_key=True),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False,
            server_default=sa.func.now()),
        sa.Column("unsupported", sa.BigInteger, nullable=False),
        sa.Column("parse_failure", sa.BigInteger, nullable=False),
        sa.Column("plan_failure", sa.BigInteger, nullable=False),
        sa.Column("inference_failure", sa.BigInteger, nullable=False),
        sa.Column("output_failure", sa.BigInteger, nullable=False),
        sa.Column("bail", sa.BigInteger, nullable=False),
        sa.Column("success", sa.BigInteger, nullable=False),
    )


def downgrade():
    op.drop_table("slt")
