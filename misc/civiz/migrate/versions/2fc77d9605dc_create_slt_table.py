# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

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
            default=sa.func.now()),
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
