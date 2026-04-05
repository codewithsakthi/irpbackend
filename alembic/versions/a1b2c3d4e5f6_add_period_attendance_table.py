"""add_period_attendance_table

Revision ID: a1b2c3d4e5f6
Revises: 41e1abf9d0ff
Create Date: 2026-03-31 08:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = '41e1abf9d0ff'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create period_attendance table for per-period, per-subject attendance tracking."""
    op.create_table(
        'period_attendance',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('student_id', sa.Integer(), sa.ForeignKey('students.id'), nullable=False),
        sa.Column('subject_id', sa.Integer(), sa.ForeignKey('subjects.id'), nullable=False),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('period', sa.SmallInteger(), nullable=False),
        sa.Column('status', sa.CHAR(1), nullable=False, server_default='P'),
        sa.Column('marked_by_faculty_id', sa.Integer(), sa.ForeignKey('staff.id'), nullable=True),
        sa.Column('is_substitute', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('created_at', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('updated_at', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP')),
    )

    # Unique constraint: one record per student per subject per period per day
    op.create_unique_constraint(
        'uq_period_attendance',
        'period_attendance',
        ['student_id', 'subject_id', 'date', 'period']
    )

    # Check constraints for data integrity
    op.create_check_constraint('chk_period_range', 'period_attendance', 'period BETWEEN 1 AND 7')
    op.create_check_constraint('chk_pa_status', 'period_attendance', "status IN ('P', 'A')")

    # Performance indexes
    op.create_index('idx_pa_date_subject', 'period_attendance', ['date', 'subject_id'])
    op.create_index('idx_pa_student_date', 'period_attendance', ['student_id', 'date'])
    op.create_index('idx_pa_faculty_date', 'period_attendance', ['marked_by_faculty_id', 'date'])


def downgrade() -> None:
    """Drop period_attendance table."""
    op.drop_index('idx_pa_faculty_date', table_name='period_attendance')
    op.drop_index('idx_pa_student_date', table_name='period_attendance')
    op.drop_index('idx_pa_date_subject', table_name='period_attendance')
    op.drop_constraint('chk_pa_status', 'period_attendance', type_='check')
    op.drop_constraint('chk_period_range', 'period_attendance', type_='check')
    op.drop_constraint('uq_period_attendance', 'period_attendance', type_='unique')
    op.drop_table('period_attendance')
