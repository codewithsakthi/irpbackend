"""add_github_access_token_to_spics

Revision ID: 1d2f0d9c6b6a
Revises: cae86a740304
Create Date: 2026-05-28

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "1d2f0d9c6b6a"
down_revision: Union[str, Sequence[str], None] = "cae86a740304"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "student_professional_profiles",
        sa.Column("github_access_token", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("student_professional_profiles", "github_access_token")
