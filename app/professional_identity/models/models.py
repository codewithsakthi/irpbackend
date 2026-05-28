"""
SPICS — SQLAlchemy ORM Models
Uses a SEPARATE declarative Base from the main app to ensure complete isolation.
These tables are additive-only: they do NOT modify any existing production tables.
"""
import enum
from sqlalchemy import (
    Column, Integer, String, Boolean, Date, ForeignKey,
    Numeric, Text, TIMESTAMP, JSON, Enum as PgEnum,
    UniqueConstraint, Index, CheckConstraint, text,
)
from sqlalchemy.orm import relationship, declarative_base
from app.core.database import Base

# ── Isolated Base — sharing metadata registry with core Base for FK resolution ────────────
ProfessionalBase = declarative_base(metadata=Base.metadata)


# ── Enums ──────────────────────────────────────────────────────────────────────

class PrimaryDomain(str, enum.Enum):
    FRONTEND    = "frontend"
    BACKEND     = "backend"
    FULLSTACK   = "fullstack"
    DATA        = "data"
    ML          = "ml"
    DEVOPS      = "devops"
    MOBILE      = "mobile"
    CYBERSEC    = "cybersecurity"
    OTHER       = "other"


class ComplexityLevel(str, enum.Enum):
    BEGINNER     = "beginner"
    INTERMEDIATE = "intermediate"
    ADVANCED     = "advanced"


class CompletionStatus(str, enum.Enum):
    IN_PROGRESS = "in_progress"
    COMPLETED   = "completed"
    ARCHIVED    = "archived"


class VerificationStatus(str, enum.Enum):
    PENDING  = "pending"
    VERIFIED = "verified"
    REJECTED = "rejected"


class SkillCategory(str, enum.Enum):
    PROGRAMMING = "programming"
    FRAMEWORK   = "framework"
    DATABASE    = "database"
    CLOUD       = "cloud"
    TOOL        = "tool"
    SOFT_SKILL  = "soft_skill"
    OTHER       = "other"


class ProficiencyLevel(str, enum.Enum):
    BEGINNER     = "beginner"
    INTERMEDIATE = "intermediate"
    ADVANCED     = "advanced"
    EXPERT       = "expert"


class SkillVerificationStatus(str, enum.Enum):
    SELF_REPORTED    = "self_reported"
    FACULTY_VERIFIED = "faculty_verified"


class CertVerificationStatus(str, enum.Enum):
    UNVERIFIED = "unverified"
    VERIFIED   = "verified"
    EXPIRED    = "expired"


class AIStatus(str, enum.Enum):
    PENDING    = "pending"
    PROCESSING = "processing"
    COMPLETED  = "completed"
    FAILED     = "failed"
    DEGRADED   = "degraded"   # AI failed but fallback summary available
    DISABLED   = "disabled"


# ── Model 1: Student Professional Profile ──────────────────────────────────────

class StudentProfessionalProfile(ProfessionalBase):
    """
    Core professional identity record for a student.
    One record per student (created by student, optionally enriched by AI).
    FK → students.id (existing table, read-only reference).
    """
    __tablename__ = "student_professional_profiles"

    student_id  = Column(
        Integer,
        ForeignKey("students.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
        comment="References existing students.id — read-only FK",
    )
    github_username       = Column(String(100), nullable=True)
    github_access_token   = Column(Text, nullable=True, comment="OAuth token for private repo access")
    portfolio_url         = Column(String(500), nullable=True)
    linkedin_url          = Column(String(500), nullable=True)
    linkedin_access_token = Column(Text, nullable=True, comment="OAuth token for LinkedIn API access")
    leetcode_username     = Column(String(100), nullable=True)
    hackerrank_username   = Column(String(100), nullable=True)
    codechef_username     = Column(String(100), nullable=True)
    primary_domain        = Column(PgEnum(PrimaryDomain, name="primary_domain_enum", values_callable=lambda x: [e.value for e in x]), nullable=True)
    bio                   = Column(String(1000), nullable=True)
    career_interest       = Column(JSON, nullable=True, comment="List[str] of interest areas")
    resume_file_path      = Column(String(500), nullable=True, comment="Relative path to uploaded resume")
    resume_uploaded_at    = Column(TIMESTAMP, nullable=True)
    picture_url           = Column(Text, nullable=True, comment="Profile picture URL (local path or remote URL)")
    github_cache_data     = Column(JSON, nullable=True, comment="Cached GitHub API response")
    github_cache_expires_at = Column(TIMESTAMP, nullable=True)
    leetcode_cache_data     = Column(JSON, nullable=True, comment="Cached LeetCode API response")
    leetcode_cache_expires_at = Column(TIMESTAMP, nullable=True)
    linkedin_cache_data    = Column(JSON, nullable=True, comment="Cached LinkedIn profile data (name, headline, picture, email)")
    profile_completion_score = Column(Numeric(5, 2), default=0.0, comment="0-100 computed score")
    is_public             = Column(Boolean, default=True, comment="Visible to faculty/admin")
    created_at            = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at            = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"),
                                   onupdate=text("CURRENT_TIMESTAMP"))

    # Relationships (within SPICS only)
    projects       = relationship("StudentProject",       back_populates="profile",
                                   primaryjoin="StudentProfessionalProfile.student_id == StudentProject.student_id",
                                   foreign_keys="[StudentProject.student_id]",
                                   cascade="all, delete-orphan", lazy="select")
    certifications = relationship("StudentCertification", back_populates="profile",
                                   primaryjoin="StudentProfessionalProfile.student_id == StudentCertification.student_id",
                                   foreign_keys="[StudentCertification.student_id]",
                                   cascade="all, delete-orphan", lazy="select")
    skills         = relationship("StudentSkill",         back_populates="profile",
                                   primaryjoin="StudentProfessionalProfile.student_id == StudentSkill.student_id",
                                   foreign_keys="[StudentSkill.student_id]",
                                   cascade="all, delete-orphan", lazy="select")
    ai_insights    = relationship("AIProfessionalInsight", back_populates="profile",
                                   primaryjoin="StudentProfessionalProfile.student_id == AIProfessionalInsight.student_id",
                                   foreign_keys="[AIProfessionalInsight.student_id]",
                                   uselist=False, cascade="all, delete-orphan", lazy="select")


# ── Model 2: Student Projects ──────────────────────────────────────────────────

class StudentProject(ProfessionalBase):
    """Project portfolio entries. Faculty can verify projects."""
    __tablename__ = "student_projects"

    project_id   = Column(Integer, primary_key=True, autoincrement=True)
    student_id   = Column(
        Integer,
        ForeignKey("students.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    title             = Column(String(255), nullable=False)
    description       = Column(Text, nullable=True)
    tech_stack        = Column(JSON, nullable=True, comment="List[str] of technologies")
    github_url        = Column(String(500), nullable=True)
    is_github_imported = Column(Boolean, default=False, comment="Project imported from GitHub (not deletable via API)")
    demo_url          = Column(String(500), nullable=True)
    role              = Column(String(100), nullable=True, comment="Student's role in project")
    team_size         = Column(Integer, default=1)
    complexity_level  = Column(PgEnum(ComplexityLevel, name="complexity_level_enum", values_callable=lambda x: [e.value for e in x]), nullable=True)
    completion_status = Column(
        PgEnum(CompletionStatus, name="completion_status_enum", values_callable=lambda x: [e.value for e in x]),
        default=CompletionStatus.IN_PROGRESS,
    )
    start_date        = Column(Date, nullable=True)
    end_date          = Column(Date, nullable=True)
    # Faculty verification
    verified_by_faculty  = Column(Integer, ForeignKey("staff.id"), nullable=True)
    verification_status  = Column(
        PgEnum(VerificationStatus, name="project_verification_status_enum", values_callable=lambda x: [e.value for e in x]),
        default=VerificationStatus.PENDING,
    )
    faculty_remarks      = Column(String(500), nullable=True)
    verified_at          = Column(TIMESTAMP, nullable=True)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"),
                        onupdate=text("CURRENT_TIMESTAMP"))

    profile = relationship("StudentProfessionalProfile", back_populates="projects",
                           primaryjoin="StudentProject.student_id == StudentProfessionalProfile.student_id",
                           foreign_keys=[student_id])

    __table_args__ = (
        CheckConstraint("team_size >= 1 AND team_size <= 50", name="chk_team_size"),
    )


# ── Model 3: Student Certifications ───────────────────────────────────────────

class StudentCertification(ProfessionalBase):
    """Industry certifications and courses. Proof upload supported."""
    __tablename__ = "student_certifications"

    certification_id = Column(Integer, primary_key=True, autoincrement=True)
    student_id       = Column(
        Integer,
        ForeignKey("students.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    title          = Column(String(255), nullable=False)
    provider       = Column(String(255), nullable=True, comment="e.g., Coursera, NPTEL, AWS")
    issue_date     = Column(Date, nullable=True)
    expiry_date    = Column(Date, nullable=True)
    credential_id  = Column(String(255), nullable=True)
    credential_url = Column(String(500), nullable=True)
    proof_file_path = Column(String(500), nullable=True, comment="Uploaded certificate image/PDF path")
    verification_status = Column(
        PgEnum(CertVerificationStatus, name="cert_verification_status_enum", values_callable=lambda x: [e.value for e in x]),
        default=CertVerificationStatus.UNVERIFIED,
    )
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"),
                        onupdate=text("CURRENT_TIMESTAMP"))

    profile = relationship("StudentProfessionalProfile", back_populates="certifications",
                           primaryjoin="StudentCertification.student_id == StudentProfessionalProfile.student_id",
                           foreign_keys=[student_id])


# ── Model 4: Student Skills ────────────────────────────────────────────────────

class StudentSkill(ProfessionalBase):
    """Skill matrix. Faculty can rate and verify self-reported skills."""
    __tablename__ = "student_skills"

    skill_id   = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(
        Integer,
        ForeignKey("students.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    skill_name        = Column(String(100), nullable=False)
    category          = Column(PgEnum(SkillCategory, name="skill_category_enum", values_callable=lambda x: [e.value for e in x]),
                               default=SkillCategory.PROGRAMMING)
    proficiency_level = Column(PgEnum(ProficiencyLevel, name="proficiency_level_enum", values_callable=lambda x: [e.value for e in x]),
                               nullable=True)
    self_rating       = Column(Integer, nullable=True, comment="1-10 self assessment")
    faculty_rating    = Column(Integer, nullable=True, comment="1-10 faculty assessment")
    faculty_rater_id  = Column(Integer, ForeignKey("staff.id"), nullable=True)
    verification_status = Column(
        PgEnum(SkillVerificationStatus, name="skill_verification_status_enum", values_callable=lambda x: [e.value for e in x]),
        default=SkillVerificationStatus.SELF_REPORTED,
    )
    years_experience  = Column(Numeric(4, 1), nullable=True, comment="e.g., 1.5 years")
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"),
                        onupdate=text("CURRENT_TIMESTAMP"))

    profile = relationship("StudentProfessionalProfile", back_populates="skills",
                           primaryjoin="StudentSkill.student_id == StudentProfessionalProfile.student_id",
                           foreign_keys=[student_id])

    __table_args__ = (
        UniqueConstraint("student_id", "skill_name", name="uq_student_skill"),
        CheckConstraint("self_rating IS NULL OR (self_rating >= 1 AND self_rating <= 10)",
                        name="chk_self_rating"),
        CheckConstraint("faculty_rating IS NULL OR (faculty_rating >= 1 AND faculty_rating <= 10)",
                        name="chk_faculty_rating"),
    )


# ── Model 5: AI Professional Insights ─────────────────────────────────────────

class AIProfessionalInsight(ProfessionalBase):
    """
    Cached AI analysis result. One record per student (upserted).
    Never blocks request flow — written by background worker.
    """
    __tablename__ = "ai_professional_insights"

    insight_id   = Column(Integer, primary_key=True, autoincrement=True)
    student_id   = Column(
        Integer,
        ForeignKey("students.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,  # one AI insight record per student
        index=True,
    )
    # Dimension scores (0-100)
    technical_depth_score   = Column(Numeric(5, 2), nullable=True)
    communication_score     = Column(Numeric(5, 2), nullable=True)
    innovation_score        = Column(Numeric(5, 2), nullable=True)
    collaboration_score     = Column(Numeric(5, 2), nullable=True)
    project_maturity_score  = Column(Numeric(5, 2), nullable=True)
    career_readiness_score  = Column(Numeric(5, 2), nullable=True)
    # AI text outputs
    ai_summary          = Column(Text, nullable=True)
    strengths           = Column(JSON, nullable=True, comment="List[str]")
    improvement_areas   = Column(JSON, nullable=True, comment="List[str]")
    career_fit_roles    = Column(JSON, nullable=True, comment="List[dict]")
    missing_skills      = Column(JSON, nullable=True, comment="List[str]")
    resume_insights     = Column(JSON, nullable=True, comment="Structured resume analysis")
    # Processing metadata
    ai_status           = Column(PgEnum(AIStatus, name="ai_status_enum", values_callable=lambda x: [e.value for e in x]),
                                  default=AIStatus.PENDING)
    generated_at        = Column(TIMESTAMP, nullable=True)
    model_used          = Column(String(100), nullable=True)
    processing_time_ms  = Column(Integer, nullable=True)
    error_detail        = Column(String(500), nullable=True, comment="Last error if status=failed")
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"),
                        onupdate=text("CURRENT_TIMESTAMP"))

    profile = relationship("StudentProfessionalProfile", back_populates="ai_insights",
                           primaryjoin="AIProfessionalInsight.student_id == StudentProfessionalProfile.student_id",
                           foreign_keys=[student_id])
