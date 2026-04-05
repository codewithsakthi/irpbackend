"""harden_student_marks

Revision ID: 41e1abf9d0ff
Revises: e8c9047c56cc
Create Date: 2026-03-17 06:45:39.961873

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '41e1abf9d0ff'
down_revision: Union[str, Sequence[str], None] = 'e8c9047c56cc'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # 1. Drop existing derived columns to re-add them as computed
    op.drop_column('student_marks', 'internal_marks')
    op.drop_column('student_marks', 'total_marks')
    op.drop_column('student_marks', 'grade')
    op.drop_column('student_marks', 'result_status')

    # 2. Re-add derived columns as computed (generated)
    op.add_column('student_marks', sa.Column('internal_marks', sa.Numeric(precision=5, scale=2), sa.Computed('GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0))', persisted=True)))
    op.add_column('student_marks', sa.Column('total_marks', sa.Numeric(precision=5, scale=2), sa.Computed('GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)', persisted=True)))
    op.add_column('student_marks', sa.Column('grade', sa.String(length=2), sa.Computed("CASE WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 90 THEN 'O' WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 80 THEN 'A+' WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 70 THEN 'A' WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 60 THEN 'B+' WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 50 THEN 'B' WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 45 THEN 'C' ELSE 'F' END", persisted=True)))
    op.add_column('student_marks', sa.Column('result_status', sa.String(length=10), sa.Computed("CASE WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 50 THEN 'Pass' ELSE 'Fail' END", persisted=True)))

    # 3. Add audit timestamps and audit trail
    op.add_column('student_marks', sa.Column('created_at', sa.TIMESTAMP(), server_default=sa.text('now()'), nullable=True))
    op.add_column('student_marks', sa.Column('updated_at', sa.TIMESTAMP(), server_default=sa.text('now()'), nullable=True))
    
    # 4. Alter column types and nullability
    op.alter_column('student_marks', 'id',
               existing_type=sa.INTEGER(),
               type_=sa.BigInteger(),
               existing_nullable=False,
               autoincrement=True)
    op.alter_column('student_marks', 'student_id',
               existing_type=sa.INTEGER(),
               nullable=False)
    op.alter_column('student_marks', 'subject_id',
               existing_type=sa.INTEGER(),
               nullable=False)
    
    # 5. Create indexes and constraints
    op.create_index('idx_sm_failed', 'student_marks', ['student_id', 'semester'], unique=False, postgresql_where=sa.text("result_status = 'Fail'"))
    op.create_index('idx_sm_student_id', 'student_marks', ['student_id'], unique=False)
    op.create_index('idx_sm_subject_semester', 'student_marks', ['subject_id', 'semester'], unique=False)
    op.create_unique_constraint('uq_student_subject_semester', 'student_marks', ['student_id', 'subject_id', 'semester'])

    # 6. Add Check Constraints
    op.create_check_constraint('chk_cit1', 'student_marks', 'cit1_marks BETWEEN 0 AND 30')
    op.create_check_constraint('chk_cit2', 'student_marks', 'cit2_marks BETWEEN 0 AND 30')
    op.create_check_constraint('chk_cit3', 'student_marks', 'cit3_marks BETWEEN 0 AND 30')
    op.create_check_constraint('chk_sem', 'student_marks', 'semester_exam_marks BETWEEN 0 AND 100')
    op.create_check_constraint('chk_semester', 'student_marks', 'semester BETWEEN 1 AND 12')


def downgrade() -> None:
    """Downgrade schema."""
    # 1. Drop constraints and indexes
    op.drop_constraint('chk_semester', 'student_marks', type_='check')
    op.drop_constraint('chk_sem', 'student_marks', type_='check')
    op.drop_constraint('chk_cit3', 'student_marks', type_='check')
    op.drop_constraint('chk_cit2', 'student_marks', type_='check')
    op.drop_constraint('chk_cit1', 'student_marks', type_='check')
    op.drop_constraint('uq_student_subject_semester', 'student_marks', type_='unique')
    op.drop_index('idx_sm_subject_semester', table_name='student_marks')
    op.drop_index('idx_sm_student_id', table_name='student_marks')
    op.drop_index('idx_sm_failed', table_name='student_marks', postgresql_where=sa.text("result_status = 'Fail'"))

    # 2. Re-alter columns
    op.alter_column('student_marks', 'subject_id',
               existing_type=sa.INTEGER(),
               nullable=True)
    op.alter_column('student_marks', 'student_id',
               existing_type=sa.INTEGER(),
               nullable=True)
    op.alter_column('student_marks', 'id',
               existing_type=sa.BigInteger(),
               type_=sa.INTEGER(),
               existing_nullable=False,
               autoincrement=True)
    
    # 3. Drop audit columns
    op.drop_column('student_marks', 'updated_at')
    op.drop_column('student_marks', 'created_at')

    # 4. Drop computed columns and re-add them as regular columns
    op.drop_column('student_marks', 'result_status')
    op.drop_column('student_marks', 'grade')
    op.drop_column('student_marks', 'total_marks')
    op.drop_column('student_marks', 'internal_marks')
    
    op.add_column('student_marks', sa.Column('internal_marks', sa.Numeric(precision=5, scale=2), nullable=True))
    op.add_column('student_marks', sa.Column('total_marks', sa.Numeric(precision=5, scale=2), nullable=True))
    op.add_column('student_marks', sa.Column('grade', sa.String(length=2), nullable=True))
    op.add_column('student_marks', sa.Column('result_status', sa.String(length=10), nullable=True))
