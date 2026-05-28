"""SPICS — GitHub Analysis, AI Insights, and Faculty Verification routers"""
import os
import logging
import httpx
from pydantic import BaseModel
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Body
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

from ...core.database import get_db
from ...core import auth
from ...models import base as core_models
from ..feature_flags import FLAGS
from ..schemas.schemas import (
    AIInsightResponse, FacultyVerifyProjectRequest,
    FacultyVerifySkillRequest, GitHubAnalysisResponse, MessageResponse,
)
from ..services.github_service import fetch_github_analysis, import_github_projects
from ..services.verification_service import VerificationService  # imported below
from ..repositories.data_repos import InsightRepository
from ..repositories.profile_repo import ProfileRepository
from ..models.models import AIStatus


# ── GitHub Router ──────────────────────────────────────────────────────────────

github_router = APIRouter(prefix="/github-analysis", tags=["SPICS — GitHub"])


@github_router.get("/{student_id}")
async def get_github_analysis(
    student_id: int,
    force_refresh: bool = False,
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    if not FLAGS.get("ENABLE_PROFESSIONAL_IDENTITY", True):
        raise HTTPException(status_code=503, detail="Module not enabled")
    if not FLAGS.get("ENABLE_GITHUB_ANALYTICS", True):
        return {"status": "disabled", "message": "GitHub analytics feature is not enabled."}

    profile = await ProfileRepository(db).get_by_student_id(student_id)
    if not profile or not profile.github_username:
        raise HTTPException(
            status_code=404,
            detail="No GitHub username linked. Student must add github_username to their profile first.",
        )
    return await fetch_github_analysis(student_id, profile.github_username, db, force_refresh)


@github_router.post("/import-projects", response_model=MessageResponse)
async def import_projects_from_github(
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    if not FLAGS.get("ENABLE_PROFESSIONAL_IDENTITY", True):
        raise HTTPException(status_code=503, detail="Module not enabled")
    student_id = _get_student_id(current_user)
    return await import_github_projects(student_id, db)


class GitHubCallbackRequest(BaseModel):
    code: str


@github_router.post("/callback", response_model=MessageResponse)
async def github_oauth_callback(
    req: GitHubCallbackRequest,
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    if not FLAGS.get("ENABLE_PROFESSIONAL_IDENTITY", True):
        raise HTTPException(status_code=503, detail="Module not enabled")

    student_id = _get_student_id(current_user)
    code = req.code

    client_id = os.getenv("GITHUB_CLIENT_ID", "ov23li4gfpa650fpfq0x")
    client_secret = os.getenv("GITHUB_CLIENT_SECRET", "mock_github_client_secret_value_here")

    # 1. Exchange code for access token
    access_token = None
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(
                "https://github.com/login/oauth/access_token",
                headers={"Accept": "application/json"},
                data={
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "code": code,
                },
                timeout=10.0,
            )
            if resp.status_code == 200:
                token_data = resp.json()
                access_token = token_data.get("access_token")
        except Exception as e:
            logger.warning(f"GitHub OAuth token exchange failed: {e}")

    if not access_token:
        raise HTTPException(
            status_code=400,
            detail="GitHub OAuth exchange failed: Invalid code, Client ID, or Client Secret. Please configure a valid GITHUB_CLIENT_SECRET in backend/.env.",
        )

    # 2. Fetch authenticated user details from GitHub
    github_username = None
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(
                "https://api.github.com/user",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Accept": "application/vnd.github+json",
                    "User-Agent": "SPARK-SPICS/1.0",
                },
                timeout=8.0,
            )
            if resp.status_code == 200:
                user_data = resp.json()
                github_username = user_data.get("login")
        except Exception as e:
            logger.warning(f"Failed to fetch GitHub user details with token: {e}")

    if not github_username:
        raise HTTPException(
            status_code=400,
            detail="Failed to retrieve authenticated GitHub username. Access token might be invalid or rate-limited.",
        )

    # 3. Update the student professional profile
    profile_repo = ProfileRepository(db)
    profile = await profile_repo.get_by_student_id(student_id)
    if not profile:
        profile = await profile_repo.create(student_id, {})
    
    await profile_repo.update(profile, {
        "github_username": github_username,
        "github_access_token": access_token,
        "picture_url": f"https://github.com/{github_username}.png",
    })

    # 4. Fetch repositories and import them automatically as projects
    try:
        from ..services.github_service import import_github_projects
        import_res = await import_github_projects(student_id, db)
        msg = f"GitHub connected as @{github_username}! {import_res['message']}"
    except Exception as e:
        msg = f"GitHub connected as @{github_username}, but project import failed: {e}"

    return MessageResponse(message=msg, detail=github_username)



# ── LeetCode Router ────────────────────────────────────────────────────────────

leetcode_router = APIRouter(prefix="/leetcode-analysis", tags=["SPICS — LeetCode"])


@leetcode_router.get("/{student_id}")
async def get_leetcode_analysis_route(
    student_id: int,
    force_refresh: bool = False,
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    if not FLAGS.get("ENABLE_PROFESSIONAL_IDENTITY", True):
        raise HTTPException(status_code=503, detail="Module not enabled")

    profile = await ProfileRepository(db).get_by_student_id(student_id)
    if not profile or not profile.leetcode_username:
        raise HTTPException(
            status_code=404,
            detail="No LeetCode username linked. Student must add leetcode_username to their profile first.",
        )
    from ..services.leetcode_service import fetch_leetcode_analysis
    return await fetch_leetcode_analysis(student_id, profile.leetcode_username, db, force_refresh)


@leetcode_router.post("/connect", response_model=MessageResponse)
async def connect_leetcode(
    username: str = Body(..., embed=True),
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    if not FLAGS.get("ENABLE_PROFESSIONAL_IDENTITY", True):
        raise HTTPException(status_code=503, detail="Module not enabled")

    student_id = _get_student_id(current_user)
    from ..services.leetcode_service import fetch_leetcode_analysis
    stats = await fetch_leetcode_analysis(student_id, username, db, force_refresh=True)
    return MessageResponse(
        message=f"LeetCode account '@{username}' successfully connected!",
        detail=f"Solved {stats.get('total_solved', 0)} questions. Rank: {stats.get('ranking', 0)}."
    )


def _get_student_id(user: core_models.User) -> int:
    if hasattr(user, "student_id") and user.student_id:
        return user.student_id
    return user.id



# ── AI Insights Router ─────────────────────────────────────────────────────────

insights_router = APIRouter(prefix="/ai-insights", tags=["SPICS — AI Insights"])


@insights_router.get("/{student_id}", response_model=AIInsightResponse)
async def get_ai_insights(
    student_id: int,
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get cached AI insights. Status field indicates processing state."""
    if not FLAGS.get("ENABLE_PROFESSIONAL_IDENTITY", True):
        raise HTTPException(status_code=503, detail="Module not enabled")

    if not FLAGS.get("ENABLE_AI_CAPABILITY_ENGINE", True):
        return AIInsightResponse(
            insight_id=0, student_id=student_id, ai_status="disabled",
            technical_depth_score=None, communication_score=None,
            innovation_score=None, collaboration_score=None,
            project_maturity_score=None, career_readiness_score=None,
            ai_summary=None, strengths=None, improvement_areas=None,
            career_fit_roles=None, missing_skills=None, resume_insights=None,
            generated_at=None, model_used=None, processing_time_ms=None,
            error_detail=None, created_at=None, updated_at=None,
        )

    insight = await InsightRepository(db).get_by_student_id(student_id)
    if not insight:
        raise HTTPException(
            status_code=404,
            detail="AI insights not yet generated. Trigger analysis via POST /profile.",
        )
    return insight


@insights_router.post("/trigger/{student_id}", response_model=MessageResponse)
async def trigger_ai_analysis(
    student_id: int,
    background_tasks: BackgroundTasks,
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Manually trigger AI analysis for a student. Runs in background."""
    if not FLAGS.get("ENABLE_PROFESSIONAL_IDENTITY", True):
        raise HTTPException(status_code=503, detail="Module not enabled")
    if not FLAGS.get("ENABLE_AI_CAPABILITY_ENGINE", True):
        return MessageResponse(message="AI engine is disabled.", detail="Set ENABLE_AI_CAPABILITY_ENGINE=true to enable.")

    # Only admin/staff or the student themselves can trigger
    _check_access(current_user, student_id)

    await InsightRepository(db).upsert_status(student_id, AIStatus.PENDING)

    from ..workers.ai_worker import run_ai_analysis
    background_tasks.add_task(run_ai_analysis, student_id, db)

    return MessageResponse(
        message="AI analysis queued.",
        detail="Results will be available via GET /ai-insights/{student_id} within 30 seconds.",
    )


# ── Faculty Verification Router ────────────────────────────────────────────────

faculty_router = APIRouter(prefix="/faculty", tags=["SPICS — Faculty Verification"])


@faculty_router.get("/pending-verifications")
async def get_pending_verifications(
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Returns all projects and skills pending faculty verification."""
    if not FLAGS.get("ENABLE_PROFESSIONAL_IDENTITY", True):
        raise HTTPException(status_code=503, detail="Module not enabled")
    _require_faculty_or_admin(current_user)
    from ..services.services import VerificationService as VS
    return await VS.get_pending_verifications(db)


@faculty_router.patch("/verify/project/{project_id}", response_model=MessageResponse)
async def verify_project(
    project_id: int,
    req: FacultyVerifyProjectRequest,
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    if not FLAGS.get("ENABLE_PROFESSIONAL_IDENTITY", True):
        raise HTTPException(status_code=503, detail="Module not enabled")
    _require_faculty_or_admin(current_user)
    from ..services.services import VerificationService as VS
    await VS.verify_project(project_id, current_user.id, req, db)
    return MessageResponse(message=f"Project {project_id} verification updated to '{req.verification_status}'")


@faculty_router.patch("/verify/skill/{skill_id}", response_model=MessageResponse)
async def verify_skill(
    skill_id: int,
    req: FacultyVerifySkillRequest,
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    if not FLAGS.get("ENABLE_PROFESSIONAL_IDENTITY", True):
        raise HTTPException(status_code=503, detail="Module not enabled")
    _require_faculty_or_admin(current_user)
    from ..services.services import VerificationService as VS
    await VS.verify_skill(skill_id, current_user.id, req, db)
    return MessageResponse(message=f"Skill {skill_id} rated and verified")


# ── Helpers ────────────────────────────────────────────────────────────────────

def _check_access(user: core_models.User, student_id: int):
    role = _get_role(user)
    if role == "student":
        uid = getattr(user, "student_id", None) or user.id
        if uid != student_id:
            raise HTTPException(status_code=403, detail="You can only trigger analysis for your own profile.")


def _require_faculty_or_admin(user: core_models.User):
    role = _get_role(user)
    if role not in ("admin", "staff", "faculty", "hod", "director"):
        raise HTTPException(status_code=403, detail="Faculty or admin access required.")


def _get_role(user: core_models.User) -> str:
    if hasattr(user, "role") and user.role:
        if hasattr(user.role, "name"):
            return user.role.name.lower()
        return str(user.role).lower()
    return "student"
