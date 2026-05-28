"""rename_student_enrollments_to_student_subject_enrollment

Revision ID: 26224786b586
Revises: b35b34fcc30e
Create Date: 2026-05-26 11:30:53.717388

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '26224786b586'
down_revision: Union[str, Sequence[str], None] = 'b35b34fcc30e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 0. Drop pre-existing empty table if it exists
    op.execute('DROP TABLE IF EXISTS student_subject_enrollment CASCADE')

    # 1. Rename table
    op.rename_table('student_enrollments', 'student_subject_enrollment')
    
    # 2. Rename indexes
    op.execute('ALTER INDEX ix_student_enrollments_student_id RENAME TO ix_student_subject_enrollment_student_id')
    op.execute('ALTER INDEX ix_student_enrollments_subject_id RENAME TO ix_student_subject_enrollment_subject_id')
    
    # 3. Rename constraints
    op.execute('ALTER TABLE student_subject_enrollment RENAME CONSTRAINT student_enrollments_pkey TO student_subject_enrollment_pkey')
    op.execute('ALTER TABLE student_subject_enrollment RENAME CONSTRAINT uq_student_enrollment TO uq_student_subject_enrollment')
    op.execute('ALTER TABLE student_subject_enrollment RENAME CONSTRAINT chk_enrollment_status TO chk_student_subject_enrollment_status')
    
    # 4. Rename foreign keys
    op.execute('ALTER TABLE student_subject_enrollment RENAME CONSTRAINT student_enrollments_student_id_fkey TO student_subject_enrollment_student_id_fkey')
    op.execute('ALTER TABLE student_subject_enrollment RENAME CONSTRAINT student_enrollments_subject_id_fkey TO student_subject_enrollment_subject_id_fkey')


def downgrade() -> None:
    # 1. Rename foreign keys back
    op.execute('ALTER TABLE student_subject_enrollment RENAME CONSTRAINT student_subject_enrollment_student_id_fkey TO student_enrollments_student_id_fkey')
    op.execute('ALTER TABLE student_subject_enrollment RENAME CONSTRAINT student_subject_enrollment_subject_id_fkey TO student_enrollments_subject_id_fkey')

    # 2. Rename constraints back
    op.execute('ALTER TABLE student_subject_enrollment RENAME CONSTRAINT student_subject_enrollment_pkey TO student_enrollments_pkey')
    op.execute('ALTER TABLE student_subject_enrollment RENAME CONSTRAINT uq_student_subject_enrollment TO uq_student_enrollment')
    op.execute('ALTER TABLE student_subject_enrollment RENAME CONSTRAINT chk_student_subject_enrollment_status TO chk_enrollment_status')

    # 3. Rename indexes back
    op.execute('ALTER INDEX ix_student_subject_enrollment_student_id RENAME TO ix_student_enrollments_student_id')
    op.execute('ALTER INDEX ix_student_subject_enrollment_subject_id RENAME TO ix_student_enrollments_subject_id')

    # 4. Rename table back
    op.rename_table('student_subject_enrollment', 'student_enrollments')
