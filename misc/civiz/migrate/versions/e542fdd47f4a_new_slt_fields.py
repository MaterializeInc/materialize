# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

from alembic import op
import sqlalchemy as sa


revision = 'e542fdd47f4a'
down_revision = '2fc77d9605dc'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("slt", sa.Column("unexpected_plan_success", sa.BigInteger))
    op.add_column("slt", sa.Column("wrong_number_of_rows_inserted", sa.BigInteger))
    op.add_column("slt", sa.Column("wrong_column_names", sa.BigInteger))


def downgrade():
    op.drop_column("slt", "unexpected_plan_success")
    op.drop_column("slt", "wrong_number_of_rows_inserted")
    op.drop_column("slt", "wrong_column_names")
