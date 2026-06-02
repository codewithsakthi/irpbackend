"""merge_main_and_asie_branches

Merges the main production branch (3545fe766bca) with the ASIE feature branch (f8fa0c5e2a68).
The ASIE tables (student_capability_scores, student_ai_profiles, student_growth_history) were
created directly in production without running the alembic migration, so we stamp the ASIE
revision as applied and merge the two branches here.

Revision ID: 12d53ac796d8
Revises: 3545fe766bca, f8fa0c5e2a68
Create Date: 2026-06-02 21:07:05.845972

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '12d53ac796d8'
down_revision: Union[str, Sequence[str], None] = ('3545fe766bca', 'f8fa0c5e2a68')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
