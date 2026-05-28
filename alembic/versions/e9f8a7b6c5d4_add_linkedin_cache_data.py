"""add_linkedin_cache_data_to_spics

Revision ID: e9f8a7b6c5d4
Revises: d61187907c9c
Create Date: 2026-05-28

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSON


# revision identifiers, used by Alembic.
revision: str = "e9f8a7b6c5d4"
down_revision: Union[str, Sequence[str], None] = "d61187907c9c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "student_professional_profiles",
        sa.Column("linkedin_cache_data", JSON(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("student_professional_profiles", "linkedin_cache_data")
