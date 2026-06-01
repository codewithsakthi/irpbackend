"""add_syllabus_tracking_tables

Revision ID: 3545fe766bca
Revises: f1e2d3c4b5a6
Create Date: 2026-06-01 11:32:19.668266

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '3545fe766bca'
down_revision: Union[str, Sequence[str], None] = 'f1e2d3c4b5a6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create syllabus_plans and syllabus_progress tables."""
    op.create_table(
        'syllabus_plans',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('subject_id', sa.Integer(), nullable=False),
        sa.Column('faculty_id', sa.Integer(), nullable=False),
        sa.Column('academic_year', sa.String(length=20), nullable=False),
        sa.Column('section', sa.String(length=20), nullable=True),
        sa.Column('unit_number', sa.Integer(), nullable=False),
        sa.Column('unit_title', sa.String(length=255), nullable=False),
        sa.Column('total_periods', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('created_at', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.Column('updated_at', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.CheckConstraint('total_periods >= 0', name='chk_syllabus_total_periods'),
        sa.ForeignKeyConstraint(['faculty_id'], ['staff.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['subject_id'], ['subjects.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'subject_id', 'faculty_id', 'academic_year', 'section', 'unit_number',
            name='uq_syllabus_plan_unit'
        ),
    )
    op.create_index('idx_syllabus_plan_faculty', 'syllabus_plans', ['faculty_id'], unique=False)
    op.create_index('idx_syllabus_plan_subject', 'syllabus_plans', ['subject_id'], unique=False)
    op.create_index(op.f('ix_syllabus_plans_id'), 'syllabus_plans', ['id'], unique=False)

    op.create_table(
        'syllabus_progress',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('plan_id', sa.Integer(), nullable=False),
        sa.Column('faculty_id', sa.Integer(), nullable=False),
        sa.Column('covered_periods', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('last_updated', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.Column('updated_by', sa.Integer(), nullable=True),
        sa.CheckConstraint('covered_periods >= 0', name='chk_syllabus_covered_periods'),
        sa.ForeignKeyConstraint(['faculty_id'], ['staff.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['plan_id'], ['syllabus_plans.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['updated_by'], ['users.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('plan_id'),
    )
    op.create_index('idx_syllabus_progress_faculty', 'syllabus_progress', ['faculty_id'], unique=False)
    op.create_index(op.f('ix_syllabus_progress_id'), 'syllabus_progress', ['id'], unique=False)


def downgrade() -> None:
    """Drop syllabus tables."""
    op.drop_index(op.f('ix_syllabus_progress_id'), table_name='syllabus_progress')
    op.drop_index('idx_syllabus_progress_faculty', table_name='syllabus_progress')
    op.drop_table('syllabus_progress')

    op.drop_index(op.f('ix_syllabus_plans_id'), table_name='syllabus_plans')
    op.drop_index('idx_syllabus_plan_subject', table_name='syllabus_plans')
    op.drop_index('idx_syllabus_plan_faculty', table_name='syllabus_plans')
    op.drop_table('syllabus_plans')
