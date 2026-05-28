"""add_linkedin_access_token_to_spics

Revision ID: a2b3c4d5e6f7
Revises: 1d2f0d9c6b6a
Create Date: 2026-05-28

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a2b3c4d5e6f7"
down_revision: Union[str, Sequence[str], None] = "1d2f0d9c6b6a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "student_professional_profiles",
        sa.Column("linkedin_access_token", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("student_professional_profiles", "linkedin_access_token")
