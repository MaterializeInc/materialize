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
