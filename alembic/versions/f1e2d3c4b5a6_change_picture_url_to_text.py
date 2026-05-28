"""change_picture_url_to_text

Revision ID: f1e2d3c4b5a6
Revises: e9f8a7b6c5d4
Create Date: 2026-05-28

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "f1e2d3c4b5a6"
down_revision: Union[str, Sequence[str], None] = "e9f8a7b6c5d4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "student_professional_profiles",
        "picture_url",
        type_=sa.Text(),
        existing_type=sa.String(1000),
        nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        "student_professional_profiles",
        "picture_url",
        type_=sa.String(1000),
        existing_type=sa.Text(),
        nullable=True,
    )
