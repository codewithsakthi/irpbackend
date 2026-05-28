"""merge_professional_identity_and_main_branches

Revision ID: d61187907c9c
Revises: a2b3c4d5e6f7, b7d4e9a1c2f3
Create Date: 2026-05-28 17:18:59.218434

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd61187907c9c'
down_revision: Union[str, Sequence[str], None] = ('a2b3c4d5e6f7', 'b7d4e9a1c2f3')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
