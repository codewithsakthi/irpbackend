"""add_performance_indexes

Revision ID: j1k2l3m4n5o6
Revises: i0j1k2l3m4n5
Create Date: 2026-05-26 00:00:00.000000

Adds critical covering indexes for the most expensive query patterns:
  - student_assessments: filtered pivot queries (is_final, student_id, assessment_type)
  - students: batch/semester/section cohort filters
  - subjects: course_code lookups
  - contact_info: student_id FK lookup
  - period_attendance: student_id aggregation (if table exists)
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'j1k2l3m4n5o6'
down_revision: Union[str, Sequence[str], None] = 'f8fa0c5e2a68'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add performance indexes for analytics queries."""

    # ── student_assessments ──────────────────────────────────────────────────
    # The marks_pivot / final_assessments CTEs always filter WHERE is_final = true
    # and GROUP BY (student_id, subject_id, semester). A partial index on the
    # final rows cuts the scan to a fraction of the table.
    op.create_index(
        'idx_sa_final_student_subject',
        'student_assessments',
        ['student_id', 'subject_id', 'semester'],
        postgresql_where=sa.text('is_final = true'),
    )

    # assessment_type is used in MAX(…) FILTER (WHERE assessment_type = …) aggregates.
    # A composite index lets Postgres use an index scan instead of a seq scan.
    op.create_index(
        'idx_sa_student_type',
        'student_assessments',
        ['student_id', 'assessment_type'],
        postgresql_where=sa.text('is_final = true'),
    )

    # ── students ─────────────────────────────────────────────────────────────
    # Cohort-filter CTEs push down batch / current_semester / section predicates.
    op.create_index(
        'idx_students_batch_sem_section',
        'students',
        ['batch', 'current_semester', 'section'],
    )

    # roll_no is used in many JOIN / WHERE conditions after id-based joins.
    # Usually already has a unique constraint but not necessarily a plain index.
    op.create_index(
        'idx_students_roll_no',
        'students',
        ['roll_no'],
        unique=False,  # unique constraint may already exist; harmless if so
        if_not_exists=True,
    )

    # ── subjects ─────────────────────────────────────────────────────────────
    # subject_catalog CTE joins on subjects.id; leaderboard / bottleneck queries
    # also filter WHERE lower(course_code) = lower(…).
    op.create_index(
        'idx_subjects_course_code_lower',
        'subjects',
        [sa.text('lower(course_code)')],
    )

    # ── contact_info ─────────────────────────────────────────────────────────
    # Every marks_enriched CTE does LEFT JOIN contact_info ci ON ci.student_id = st.id
    op.create_index(
        'idx_contact_info_student_id',
        'contact_info',
        ['student_id'],
    )


def downgrade() -> None:
    """Remove performance indexes."""
    op.drop_index('idx_contact_info_student_id', table_name='contact_info')
    op.drop_index('idx_subjects_course_code_lower', table_name='subjects')
    op.drop_index('idx_students_roll_no', table_name='students')
    op.drop_index('idx_students_batch_sem_section', table_name='students')
    op.drop_index('idx_sa_student_type', table_name='student_assessments')
    op.drop_index('idx_sa_final_student_subject', table_name='student_assessments')
