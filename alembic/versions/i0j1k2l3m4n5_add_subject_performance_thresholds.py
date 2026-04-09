"""add subject performance thresholds

Revision ID: i0j1k2l3m4n5
Revises: h9i0j1k2l3m4
Create Date: 2026-04-08 12:47:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'i0j1k2l3m4n5'
down_revision: Union[str, None] = 'h9i0j1k2l3m4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add performance threshold columns to subjects table for hybrid evaluation system."""
    
    # Add threshold configuration columns
    op.add_column('subjects', sa.Column('pass_threshold', sa.NUMERIC(5, 2), nullable=False, server_default='50.0', comment='Minimum marks to pass (default: 50)'))
    op.add_column('subjects', sa.Column('target_average', sa.NUMERIC(5, 2), nullable=True, comment='Target average for good performance'))
    op.add_column('subjects', sa.Column('percentile_excellent', sa.NUMERIC(5, 2), nullable=False, server_default='85.0', comment='Minimum percentile for "Excellent" (default: 85)'))
    op.add_column('subjects', sa.Column('percentile_good', sa.NUMERIC(5, 2), nullable=False, server_default='60.0', comment='Minimum percentile for "Good" (default: 60)'))
    op.add_column('subjects', sa.Column('percentile_average', sa.NUMERIC(5, 2), nullable=False, server_default='30.0', comment='Minimum percentile for "Average" (default: 30)'))
    
    # Add check constraints to ensure valid threshold ranges
    op.create_check_constraint(
        'chk_pass_threshold_range', 
        'subjects',
        'pass_threshold >= 0 AND pass_threshold <= 100'
    )
    
    op.create_check_constraint(
        'chk_target_average_range', 
        'subjects',
        'target_average IS NULL OR (target_average >= 0 AND target_average <= 100)'
    )
    
    op.create_check_constraint(
        'chk_percentile_hierarchy', 
        'subjects',
        'percentile_excellent >= percentile_good AND percentile_good >= percentile_average AND percentile_average >= 0 AND percentile_excellent <= 100'
    )


def downgrade() -> None:
    """Remove performance threshold columns from subjects table."""
    
    # Drop check constraints first
    op.drop_constraint('chk_percentile_hierarchy', 'subjects', type_='check')
    op.drop_constraint('chk_target_average_range', 'subjects', type_='check')
    op.drop_constraint('chk_pass_threshold_range', 'subjects', type_='check')
    
    # Drop threshold columns
    op.drop_column('subjects', 'percentile_average')
    op.drop_column('subjects', 'percentile_good')
    op.drop_column('subjects', 'percentile_excellent')
    op.drop_column('subjects', 'target_average')
    op.drop_column('subjects', 'pass_threshold')