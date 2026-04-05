"""fix_grade_calc_non_graded_subjects

Revision ID: f7g8h9i0j1k2
Revises: a1b2c3d4e5f6
Create Date: 2026-03-31 10:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f7g8h9i0j1k2'
down_revision: Union[str, Sequence[str], None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Fix grade and result_status computed columns to handle non-graded subjects (audit courses, labs).
    
    Audit courses and labs with NULL marks for all components should show:
    - grade = 'P' (Pass) instead of 'F'
    - result_status = 'Pass' instead of 'Fail'
    """
    # In PostgreSQL, computed columns must be dropped and recreated with new logic
    # Drop the old computed columns
    op.execute("ALTER TABLE student_marks DROP COLUMN grade")
    op.execute("ALTER TABLE student_marks DROP COLUMN result_status")
    
    # Recreate the computed columns with the corrected logic
    op.add_column('student_marks', sa.Column(
        'grade',
        sa.String(2),
        sa.Computed(
            """CASE 
                WHEN (cit1_marks IS NULL AND cit2_marks IS NULL AND cit3_marks IS NULL AND semester_exam_marks IS NULL) THEN 'P'
                WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 90 THEN 'O'
                WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 80 THEN 'A+'
                WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 70 THEN 'A'
                WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 60 THEN 'B+'
                WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 50 THEN 'B'
                WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 45 THEN 'C'
                ELSE 'F' 
            END""",
            persisted=True
        )
    ))
    
    op.add_column('student_marks', sa.Column(
        'result_status',
        sa.String(10),
        sa.Computed(
            "CASE WHEN (cit1_marks IS NULL AND cit2_marks IS NULL AND cit3_marks IS NULL AND semester_exam_marks IS NULL) THEN 'Pass' WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 50 THEN 'Pass' ELSE 'Fail' END",
            persisted=True
        )
    ))


def downgrade() -> None:
    """Revert to the old grade and result_status computed columns logic."""
    # Drop the new computed columns
    op.execute("ALTER TABLE student_marks DROP COLUMN grade")
    op.execute("ALTER TABLE student_marks DROP COLUMN result_status")
    
    # Recreate the old computed columns (without NULL check)
    op.add_column('student_marks', sa.Column(
        'grade',
        sa.String(2),
        sa.Computed(
            """CASE 
                WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 90 THEN 'O'
                WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 80 THEN 'A+'
                WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 70 THEN 'A'
                WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 60 THEN 'B+'
                WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 50 THEN 'B'
                WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 45 THEN 'C'
                ELSE 'F' 
            END""",
            persisted=True
        )
    ))
    
    op.add_column('student_marks', sa.Column(
        'result_status',
        sa.String(10),
        sa.Computed(
            "CASE WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 50 THEN 'Pass' ELSE 'Fail' END",
            persisted=True
        )
    ))
