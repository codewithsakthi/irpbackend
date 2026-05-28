"""
SPICS — Pydantic v2 Schemas
All schemas use from_attributes=True for ORM → schema coercion.
"""
from __future__ import annotations
from datetime import date, datetime
from typing import Any, List, Optional
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from ..models.models import (
    AIStatus, CertVerificationStatus, ComplexityLevel, CompletionStatus,
    PrimaryDomain, ProficiencyLevel, SkillCategory, SkillVerificationStatus,
    VerificationStatus,
)


# ── Base Config ────────────────────────────────────────────────────────────────

class SPICSBase(BaseModel):
    model_config = ConfigDict(from_attributes=True, use_enum_values=True)


# ── Professional Profile ───────────────────────────────────────────────────────

class ProfileCreateRequest(BaseModel):
    github_username:      Optional[str] = Field(None, max_length=100)
    portfolio_url:        Optional[str] = Field(None, max_length=500)
    linkedin_url:         Optional[str] = Field(None, max_length=500)
    leetcode_username:    Optional[str] = Field(None, max_length=100)
    hackerrank_username:  Optional[str] = Field(None, max_length=100)
    codechef_username:    Optional[str] = Field(None, max_length=100)
    primary_domain:       Optional[PrimaryDomain] = None
    bio:                  Optional[str] = Field(None, max_length=1000)
    career_interest:      Optional[List[str]] = None
    is_public:            bool = True

    @field_validator("github_username", mode="before")
    @classmethod
    def strip_github_prefix(cls, v):
        if v and "github.com/" in v:
            return v.split("github.com/")[-1].strip("/")
        return v


class ProfileUpdateRequest(ProfileCreateRequest):
    pass


class ProfileResponse(SPICSBase):
    student_id:              int
    github_username:         Optional[str]
    portfolio_url:           Optional[str]
    linkedin_url:            Optional[str]
    leetcode_username:       Optional[str]
    hackerrank_username:     Optional[str]
    codechef_username:       Optional[str]
    primary_domain:          Optional[str]
    bio:                     Optional[str]
    career_interest:         Optional[List[str]]
    resume_file_path:        Optional[str]
    resume_uploaded_at:      Optional[datetime]
    leetcode_cache_data:     Optional[Any] = None
    leetcode_cache_expires_at: Optional[datetime] = None
    profile_completion_score: Optional[float]
    is_public:               bool
    created_at:              Optional[datetime]
    updated_at:              Optional[datetime]


# ── Projects ───────────────────────────────────────────────────────────────────

class ProjectCreateRequest(BaseModel):
    title:             str = Field(..., min_length=3, max_length=255)
    description:       Optional[str] = None
    tech_stack:        Optional[List[str]] = None
    github_url:        Optional[str] = Field(None, max_length=500)
    demo_url:          Optional[str] = Field(None, max_length=500)
    role:              Optional[str] = Field(None, max_length=100)
    team_size:         int = Field(1, ge=1, le=50)
    complexity_level:  Optional[ComplexityLevel] = None
    completion_status: CompletionStatus = CompletionStatus.IN_PROGRESS
    start_date:        Optional[date] = None
    end_date:          Optional[date] = None


class ProjectUpdateRequest(ProjectCreateRequest):
    title: Optional[str] = Field(None, min_length=3, max_length=255)


class ProjectResponse(SPICSBase):
    project_id:           int
    student_id:           int
    title:                str
    description:          Optional[str]
    tech_stack:           Optional[List[str]]
    github_url:           Optional[str]
    demo_url:             Optional[str]
    role:                 Optional[str]
    team_size:            int
    complexity_level:     Optional[str]
    completion_status:    str
    start_date:           Optional[date]
    end_date:             Optional[date]
    verified_by_faculty:  Optional[int]
    verification_status:  str
    faculty_remarks:      Optional[str]
    verified_at:          Optional[datetime]
    created_at:           Optional[datetime]
    updated_at:           Optional[datetime]


# ── Certifications ─────────────────────────────────────────────────────────────

class CertificationCreateRequest(BaseModel):
    title:          str = Field(..., min_length=2, max_length=255)
    provider:       Optional[str] = Field(None, max_length=255)
    issue_date:     Optional[date] = None
    expiry_date:    Optional[date] = None
    credential_id:  Optional[str] = Field(None, max_length=255)
    credential_url: Optional[str] = Field(None, max_length=500)

    @field_validator("credential_url", mode="before")
    @classmethod
    def normalize_credential_url(cls, value):
        if not value:
            return value
        value = str(value).strip()
        if value and not value.startswith(("http://", "https://")):
            return f"https://{value}"
        return value


class CertificationUpdateRequest(BaseModel):
    title:          Optional[str] = Field(None, min_length=2, max_length=255)
    provider:       Optional[str] = Field(None, max_length=255)
    issue_date:     Optional[date] = None
    expiry_date:    Optional[date] = None
    credential_id:  Optional[str] = Field(None, max_length=255)
    credential_url: Optional[str] = Field(None, max_length=500)

    @field_validator("credential_url", mode="before")
    @classmethod
    def normalize_credential_url(cls, value):
        if not value:
            return value
        value = str(value).strip()
        if value and not value.startswith(("http://", "https://")):
            return f"https://{value}"
        return value


class CertificationResponse(SPICSBase):
    certification_id:    int
    student_id:          int
    title:               str
    provider:            Optional[str]
    issue_date:          Optional[date]
    expiry_date:         Optional[date]
    credential_id:       Optional[str]
    credential_url:      Optional[str]
    proof_file_path:     Optional[str]
    verification_status: str
    created_at:          Optional[datetime]


# ── Skills ─────────────────────────────────────────────────────────────────────

class SkillCreateRequest(BaseModel):
    skill_name:        str = Field(..., min_length=1, max_length=100)
    category:          SkillCategory = SkillCategory.PROGRAMMING
    proficiency_level: Optional[ProficiencyLevel] = None
    self_rating:       Optional[int] = Field(None, ge=1, le=10)
    years_experience:  Optional[float] = Field(None, ge=0, le=50)


class SkillUpdateRequest(SkillCreateRequest):
    skill_name: Optional[str] = Field(None, min_length=1, max_length=100)


class SkillResponse(SPICSBase):
    skill_id:            int
    student_id:          int
    skill_name:          str
    category:            str
    proficiency_level:   Optional[str]
    self_rating:         Optional[int]
    faculty_rating:      Optional[int]
    faculty_rater_id:    Optional[int]
    verification_status: str
    years_experience:    Optional[float]
    created_at:          Optional[datetime]
    updated_at:          Optional[datetime]


# ── AI Insights ────────────────────────────────────────────────────────────────

class AIInsightResponse(SPICSBase):
    insight_id:              int
    student_id:              int
    technical_depth_score:   Optional[float]
    communication_score:     Optional[float]
    innovation_score:        Optional[float]
    collaboration_score:     Optional[float]
    project_maturity_score:  Optional[float]
    career_readiness_score:  Optional[float]
    ai_summary:              Optional[str]
    strengths:               Optional[List[str]]
    improvement_areas:       Optional[List[str]]
    career_fit_roles:        Optional[List[Any]]
    missing_skills:          Optional[List[str]]
    resume_insights:         Optional[Any]
    ai_status:               str
    generated_at:            Optional[datetime]
    model_used:              Optional[str]
    processing_time_ms:      Optional[int]
    error_detail:            Optional[str]
    created_at:              Optional[datetime]
    updated_at:              Optional[datetime]


# ── Faculty Verification ───────────────────────────────────────────────────────

class FacultyVerifyProjectRequest(BaseModel):
    verification_status: VerificationStatus
    faculty_remarks:     Optional[str] = Field(None, max_length=500)


class FacultyVerifySkillRequest(BaseModel):
    faculty_rating:      int = Field(..., ge=1, le=10)
    verification_status: SkillVerificationStatus = SkillVerificationStatus.FACULTY_VERIFIED


class PendingVerificationItem(BaseModel):
    entity_type:  str   # "project" | "skill"
    entity_id:    int
    student_id:   int
    student_name: Optional[str]
    roll_no:      Optional[str]
    title:        str
    status:       str
    created_at:   Optional[datetime]


# ── GitHub Analysis ────────────────────────────────────────────────────────────

class GitHubRepoSummary(BaseModel):
    name:         str
    description:  Optional[str]
    language:     Optional[str]
    stars:        int
    forks:        int
    url:          str
    updated_at:   Optional[str]


class GitHubAnalysisResponse(BaseModel):
    student_id:           int
    github_username:      str
    public_repos:         int
    followers:            int
    following:            int
    account_created:      Optional[str]
    top_languages:        dict
    total_stars:          int
    top_repos:            List[GitHubRepoSummary]
    contribution_activity: Optional[str]
    analysis_note:        Optional[str]
    cached:               bool
    fetched_at:           Optional[datetime]


# ── Resume ─────────────────────────────────────────────────────────────────────

class ResumeUploadResponse(BaseModel):
    student_id:    int
    file_path:     str
    file_size_kb:  float
    uploaded_at:   datetime
    analysis_queued: bool
    message:       str


# ── Career Readiness ──────────────────────────────────────────────────────────

class CareerReadinessResponse(BaseModel):
    student_id:               int
    profile_completion_score: float
    total_projects:           int
    verified_projects:        int
    total_certifications:     int
    total_skills:             int
    verified_skills:          int
    has_github:               bool
    has_resume:               bool
    ai_career_readiness_score: Optional[float]
    readiness_band:           str   # "Ready" / "Near Ready" / "Building" / "Early Stage"
    top_skills:               List[str]
    recommended_next_steps:   List[str]


# ── Common Responses ──────────────────────────────────────────────────────────

class MessageResponse(BaseModel):
    message: str
    detail:  Optional[str] = None


class PaginatedResponse(BaseModel):
    total:  int
    limit:  int
    offset: int
    items:  List[Any]
