"""change credits column to decimal

Revision ID: h9i0j1k2l3m4
Revises: g8h9i0j1k2l3
Create Date: 2026-04-05 06:51:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'h9i0j1k2l3m4'
down_revision: Union[str, None] = 'g8h9i0j1k2l3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Change credits column from integer to decimal(4,2) for fractional credit support."""
    
    # Change the credits column in subjects table from integer to decimal(4,2)
    # This allows up to 99.99 credits per subject with 2 decimal places
    op.alter_column(
        'subjects',
        'credits',
        existing_type=sa.INTEGER(),
        type_=sa.NUMERIC(4, 2),
        existing_nullable=True,
        postgresql_using='credits::numeric(4,2)'
    )


def downgrade() -> None:
    """Revert credits column from decimal back to integer."""
    
    # Convert back to integer, rounding any decimal values
    op.alter_column(
        'subjects', 
        'credits',
        existing_type=sa.NUMERIC(4, 2),
        type_=sa.INTEGER(),
        existing_nullable=True,
        postgresql_using='ROUND(credits)::integer'
    )