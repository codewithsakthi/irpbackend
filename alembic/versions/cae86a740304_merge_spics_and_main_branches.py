"""merge_spics_and_main_branches

Revision ID: cae86a740304
Revises: 7e0e046d7d67, j1k2l3m4n5o6
Create Date: 2026-05-26 11:17:54.274539

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'cae86a740304'
down_revision: Union[str, Sequence[str], None] = ('7e0e046d7d67', 'j1k2l3m4n5o6')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
