# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

from alembic import op
import sqlalchemy as sa


revision = 'c8a6f3f3a2ac'
down_revision = 'e542fdd47f4a'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column("slt", "wrong_number_of_rows_inserted",
                    new_column_name="wrong_number_of_rows_affected")


def downgrade():
    op.alter_column("slt", "wrong_number_of_rows_affected",
                    new_column_name="wrong_number_of_rows_inserted")
