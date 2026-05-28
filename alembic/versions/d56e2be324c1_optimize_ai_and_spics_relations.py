"""optimize_ai_and_spics_relations

Revision ID: d56e2be324c1
Revises: 26224786b586
Create Date: 2026-05-26 11:41:33.727945

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd56e2be324c1'
down_revision: Union[str, Sequence[str], None] = '26224786b586'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Add columns to student_capability_scores
    op.add_column('student_capability_scores', sa.Column('placement_probability', sa.Numeric(precision=5, scale=2), nullable=True))
    op.add_column('student_capability_scores', sa.Column('confidence_score', sa.Numeric(precision=4, scale=2), nullable=True))
    op.add_column('student_capability_scores', sa.Column('computed_at', sa.TIMESTAMP(), nullable=True))

    # 2. Backfill placement_probability & confidence_score to student_capability_scores from student_ai_profiles
    op.execute("""
        UPDATE student_capability_scores sc
        SET placement_probability = ai.placement_probability,
            confidence_score = ai.confidence_score,
            computed_at = ai.generated_at
        FROM student_ai_profiles ai
        WHERE sc.student_id = ai.student_id
    """)
    op.execute("UPDATE student_capability_scores SET computed_at = CURRENT_TIMESTAMP WHERE computed_at IS NULL")

    # 3. Normalize SPICS Foreign Key Constraints pointing directly to students.id (done first to prevent FK violation on data migration)
    for t in ['student_projects', 'student_certifications', 'student_skills', 'ai_professional_insights']:
        fk_name = f"{t}_student_id_fkey"
        op.drop_constraint(fk_name, t, type_='foreignkey')
        op.create_foreign_key(fk_name, t, 'students', ['student_id'], ['id'], ondelete='CASCADE')

    # 4. Copy qualitative AI profile data from student_ai_profiles into ai_professional_insights
    op.execute("""
        INSERT INTO ai_professional_insights (student_id, strengths, improvement_areas, missing_skills, career_fit_roles, ai_summary, generated_at, ai_status)
        SELECT 
            student_id, strengths, weaknesses, recommendations, career_fit, ai_summary, generated_at, 'completed'
        FROM student_ai_profiles
        ON CONFLICT (student_id) DO UPDATE
        SET strengths = EXCLUDED.strengths,
            improvement_areas = EXCLUDED.improvement_areas,
            missing_skills = EXCLUDED.missing_skills,
            career_fit_roles = EXCLUDED.career_fit_roles,
            ai_summary = EXCLUDED.ai_summary,
            generated_at = EXCLUDED.generated_at,
            ai_status = 'completed'
    """)

    # 5. Drop student_ai_profiles table
    op.drop_table('student_ai_profiles')


def downgrade() -> None:
    # 1. Restore SPICS Foreign Key Constraints pointing to student_professional_profiles.student_id
    for t in ['student_projects', 'student_certifications', 'student_skills', 'ai_professional_insights']:
        fk_name = f"{t}_student_id_fkey"
        op.drop_constraint(fk_name, t, type_='foreignkey')
        op.create_foreign_key(fk_name, t, 'student_professional_profiles', ['student_id'], ['student_id'], ondelete='CASCADE')

    # 2. Recreate student_ai_profiles table
    op.create_table(
        'student_ai_profiles',
        sa.Column('student_id', sa.Integer(), sa.ForeignKey('students.id', ondelete='CASCADE'), primary_key=True),
        sa.Column('primary_identity', sa.String(length=100), nullable=True),
        sa.Column('secondary_identity', sa.String(length=100), nullable=True),
        sa.Column('strengths', sa.JSON(), nullable=True),
        sa.Column('weaknesses', sa.JSON(), nullable=True),
        sa.Column('recommendations', sa.JSON(), nullable=True),
        sa.Column('placement_probability', sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column('career_fit', sa.JSON(), nullable=True),
        sa.Column('ai_summary', sa.String(), nullable=True),
        sa.Column('confidence_score', sa.Numeric(precision=4, scale=2), nullable=True),
        sa.Column('generated_at', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'))
    )

    # 3. Restore data to student_ai_profiles from capability scores and professional insights
    op.execute("""
        INSERT INTO student_ai_profiles (student_id, strengths, weaknesses, recommendations, career_fit, ai_summary, placement_probability, confidence_score, generated_at)
        SELECT 
            sc.student_id,
            pi.strengths,
            pi.improvement_areas,
            pi.missing_skills,
            pi.career_fit_roles,
            pi.ai_summary,
            sc.placement_probability,
            sc.confidence_score,
            COALESCE(sc.computed_at, CURRENT_TIMESTAMP)
        FROM student_capability_scores sc
        LEFT JOIN ai_professional_insights pi ON sc.student_id = pi.student_id
    """)

    # 4. Remove columns from student_capability_scores
    op.drop_column('student_capability_scores', 'computed_at')
    op.drop_column('student_capability_scores', 'confidence_score')
    op.drop_column('student_capability_scores', 'placement_probability')
