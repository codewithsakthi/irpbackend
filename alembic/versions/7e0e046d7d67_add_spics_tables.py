"""
SPICS: Add student professional identity tables

Revision ID: 7e0e046d7d67
Revises: (auto — place your last migration revision here)
Create Date: 2026-05-26

SAFETY: This migration ONLY creates new tables. It does NOT modify any existing tables.
Rollback: downgrade() drops all 5 new tables cleanly.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision = "7e0e046d7d67"
down_revision = None   # ← replace with your actual latest revision ID
branch_labels = ("professional_identity",)
depends_on = None


def upgrade() -> None:
    # ── Safe Enums Creation ──────────────────────────────────────────────────
    for enum_name, enum_values in [
        ("primary_domain_enum", ["frontend", "backend", "fullstack", "data", "ml", "devops", "mobile", "cybersecurity", "other"]),
        ("complexity_level_enum", ["beginner", "intermediate", "advanced"]),
        ("completion_status_enum", ["in_progress", "completed", "archived"]),
        ("project_verification_status_enum", ["pending", "verified", "rejected"]),
        ("cert_verification_status_enum", ["unverified", "verified", "expired"]),
        ("skill_category_enum", ["programming", "framework", "database", "cloud", "tool", "soft_skill", "other"]),
        ("proficiency_level_enum", ["beginner", "intermediate", "advanced", "expert"]),
        ("skill_verification_status_enum", ["self_reported", "faculty_verified"]),
        ("ai_status_enum", ["pending", "processing", "completed", "failed", "degraded", "disabled"])
    ]:
        values_str = ", ".join(f"'{v}'" for v in enum_values)
        op.execute(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '{enum_name}') THEN
                    CREATE TYPE {enum_name} AS ENUM ({values_str});
                END IF;
            END$$;
        """)

    # ── Table 1: student_professional_profiles ──────────────────────────────────
    op.create_table(
        "student_professional_profiles",
        sa.Column("student_id", sa.Integer(), sa.ForeignKey("students.id", ondelete="CASCADE"), primary_key=True, nullable=False),
        sa.Column("github_username",       sa.String(100), nullable=True),
        sa.Column("portfolio_url",         sa.String(500), nullable=True),
        sa.Column("linkedin_url",          sa.String(500), nullable=True),
        sa.Column("leetcode_username",     sa.String(100), nullable=True),
        sa.Column("hackerrank_username",   sa.String(100), nullable=True),
        sa.Column("codechef_username",     sa.String(100), nullable=True),
        sa.Column("primary_domain",        postgresql.ENUM(name="primary_domain_enum", create_type=False), nullable=True),
        sa.Column("bio",                   sa.String(1000), nullable=True),
        sa.Column("career_interest",       sa.JSON, nullable=True),
        sa.Column("resume_file_path",      sa.String(500), nullable=True),
        sa.Column("resume_uploaded_at",    sa.TIMESTAMP, nullable=True),
        sa.Column("github_cache_data",     sa.JSON, nullable=True),
        sa.Column("github_cache_expires_at", sa.TIMESTAMP, nullable=True),
        sa.Column("profile_completion_score", sa.Numeric(5, 2), server_default="0.0"),
        sa.Column("is_public",             sa.Boolean, server_default="true"),
        sa.Column("created_at",            sa.TIMESTAMP, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at",            sa.TIMESTAMP, server_default=sa.text("CURRENT_TIMESTAMP")),
    )
    op.create_index("ix_spp_student_id", "student_professional_profiles", ["student_id"])

    # ── Table 2: student_projects ───────────────────────────────────────────────
    op.create_table(
        "student_projects",
        sa.Column("project_id",  sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column("student_id",  sa.Integer(), sa.ForeignKey("student_professional_profiles.student_id", ondelete="CASCADE"), nullable=False),
        sa.Column("title",             sa.String(255), nullable=False),
        sa.Column("description",       sa.Text,        nullable=True),
        sa.Column("tech_stack",        sa.JSON,        nullable=True),
        sa.Column("github_url",        sa.String(500), nullable=True),
        sa.Column("demo_url",          sa.String(500), nullable=True),
        sa.Column("role",              sa.String(100), nullable=True),
        sa.Column("team_size",         sa.Integer,     server_default="1"),
        sa.Column("complexity_level",  postgresql.ENUM(name="complexity_level_enum", create_type=False), nullable=True),
        sa.Column("completion_status", postgresql.ENUM(name="completion_status_enum", create_type=False), server_default="in_progress"),
        sa.Column("start_date",        sa.Date,        nullable=True),
        sa.Column("end_date",          sa.Date,        nullable=True),
        sa.Column("verified_by_faculty", sa.Integer,   sa.ForeignKey("staff.id"), nullable=True),
        sa.Column("verification_status", postgresql.ENUM(name="project_verification_status_enum", create_type=False), server_default="pending"),
        sa.Column("faculty_remarks",   sa.String(500), nullable=True),
        sa.Column("verified_at",       sa.TIMESTAMP,   nullable=True),
        sa.Column("created_at",        sa.TIMESTAMP,   server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at",        sa.TIMESTAMP,   server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.CheckConstraint("team_size >= 1 AND team_size <= 50", name="chk_team_size"),
    )
    op.create_index("ix_sp_student_id", "student_projects", ["student_id"])

    # ── Table 3: student_certifications ────────────────────────────────────────
    op.create_table(
        "student_certifications",
        sa.Column("certification_id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column("student_id",   sa.Integer(), sa.ForeignKey("student_professional_profiles.student_id", ondelete="CASCADE"), nullable=False),
        sa.Column("title",            sa.String(255), nullable=False),
        sa.Column("provider",         sa.String(255), nullable=True),
        sa.Column("issue_date",       sa.Date,        nullable=True),
        sa.Column("expiry_date",      sa.Date,        nullable=True),
        sa.Column("credential_id",    sa.String(255), nullable=True),
        sa.Column("credential_url",   sa.String(500), nullable=True),
        sa.Column("proof_file_path",  sa.String(500), nullable=True),
        sa.Column("verification_status", postgresql.ENUM(name="cert_verification_status_enum", create_type=False), server_default="unverified"),
        sa.Column("created_at",       sa.TIMESTAMP,   server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at",       sa.TIMESTAMP,   server_default=sa.text("CURRENT_TIMESTAMP")),
    )
    op.create_index("ix_sc_student_id", "student_certifications", ["student_id"])

    # ── Table 4: student_skills ─────────────────────────────────────────────────
    op.create_table(
        "student_skills",
        sa.Column("skill_id",    sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column("student_id",  sa.Integer(), sa.ForeignKey("student_professional_profiles.student_id", ondelete="CASCADE"), nullable=False),
        sa.Column("skill_name",        sa.String(100), nullable=False),
        sa.Column("category",          postgresql.ENUM(name="skill_category_enum", create_type=False), server_default="programming"),
        sa.Column("proficiency_level", postgresql.ENUM(name="proficiency_level_enum", create_type=False), nullable=True),
        sa.Column("self_rating",       sa.Integer,  nullable=True),
        sa.Column("faculty_rating",    sa.Integer,  nullable=True),
        sa.Column("faculty_rater_id",  sa.Integer,  sa.ForeignKey("staff.id"), nullable=True),
        sa.Column("verification_status", postgresql.ENUM(name="skill_verification_status_enum", create_type=False), server_default="self_reported"),
        sa.Column("years_experience",  sa.Numeric(4, 1), nullable=True),
        sa.Column("created_at",        sa.TIMESTAMP, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at",        sa.TIMESTAMP, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.UniqueConstraint("student_id", "skill_name", name="uq_student_skill"),
        sa.CheckConstraint("self_rating IS NULL OR (self_rating >= 1 AND self_rating <= 10)", name="chk_self_rating"),
        sa.CheckConstraint("faculty_rating IS NULL OR (faculty_rating >= 1 AND faculty_rating <= 10)", name="chk_faculty_rating"),
    )
    op.create_index("ix_ss_student_id", "student_skills", ["student_id"])

    # ── Table 5: ai_professional_insights ──────────────────────────────────────
    op.create_table(
        "ai_professional_insights",
        sa.Column("insight_id",  sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column("student_id",  sa.Integer(), sa.ForeignKey("student_professional_profiles.student_id", ondelete="CASCADE"), nullable=False, unique=True),
        sa.Column("technical_depth_score",   sa.Numeric(5, 2), nullable=True),
        sa.Column("communication_score",     sa.Numeric(5, 2), nullable=True),
        sa.Column("innovation_score",        sa.Numeric(5, 2), nullable=True),
        sa.Column("collaboration_score",     sa.Numeric(5, 2), nullable=True),
        sa.Column("project_maturity_score",  sa.Numeric(5, 2), nullable=True),
        sa.Column("career_readiness_score",  sa.Numeric(5, 2), nullable=True),
        sa.Column("ai_summary",          sa.Text,    nullable=True),
        sa.Column("strengths",           sa.JSON,    nullable=True),
        sa.Column("improvement_areas",   sa.JSON,    nullable=True),
        sa.Column("career_fit_roles",    sa.JSON,    nullable=True),
        sa.Column("missing_skills",      sa.JSON,    nullable=True),
        sa.Column("resume_insights",     sa.JSON,    nullable=True),
        sa.Column("ai_status",           postgresql.ENUM(name="ai_status_enum", create_type=False), server_default="pending"),
        sa.Column("generated_at",        sa.TIMESTAMP, nullable=True),
        sa.Column("model_used",          sa.String(100), nullable=True),
        sa.Column("processing_time_ms",  sa.Integer,   nullable=True),
        sa.Column("error_detail",        sa.String(500), nullable=True),
        sa.Column("created_at",          sa.TIMESTAMP, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at",          sa.TIMESTAMP, server_default=sa.text("CURRENT_TIMESTAMP")),
    )
    op.create_index("ix_api_student_id", "ai_professional_insights", ["student_id"])


def downgrade() -> None:
    """Drops all 5 SPICS tables. Existing production tables are unaffected."""
    op.drop_table("ai_professional_insights")
    op.drop_table("student_skills")
    op.drop_table("student_certifications")
    op.drop_table("student_projects")
    op.drop_table("student_professional_profiles")

    # Drop enums (PostgreSQL-specific)
    for enum_name in [
        "ai_status_enum", "skill_verification_status_enum", "proficiency_level_enum",
        "skill_category_enum", "cert_verification_status_enum",
        "project_verification_status_enum", "completion_status_enum",
        "complexity_level_enum", "primary_domain_enum",
    ]:
        op.execute(f"DROP TYPE IF EXISTS {enum_name}")
