"""SPICS — Background AI worker.
Called via FastAPI BackgroundTasks — NEVER blocks the HTTP response.
All failures are caught and stored in ai_professional_insights with appropriate status.
"""
import logging
import time
from sqlalchemy.ext.asyncio import AsyncSession

from ..models.models import AIStatus
from ..repositories.data_repos import (
    InsightRepository, ProjectRepository,
    CertificationRepository, SkillRepository,
)
from ..repositories.profile_repo import ProfileRepository
from ..ai.capability_analyzer import analyze_student_profile, analyze_resume
from ..ai.fallbacks import generate_fallback_summary
from ..feature_flags import FLAGS

logger = logging.getLogger(__name__)


async def run_ai_analysis(student_id: int, db: AsyncSession) -> None:
    """
    Main background worker entry point.
    Called AFTER the HTTP response is sent — failures are invisible to the user.
    """
    if not FLAGS.get("ENABLE_AI_CAPABILITY_ENGINE", True):
        logger.info(f"SPICS AI: Engine disabled by flag — skipping student {student_id}")
        return

    insight_repo = InsightRepository(db)
    try:
        # Mark as processing
        await insight_repo.upsert_status(student_id, AIStatus.PROCESSING)

        # Gather context
        context = await _build_analysis_context(student_id, db)
        start   = time.monotonic()

        # Call AI
        ai_result = await analyze_student_profile(context)
        elapsed_ms = int((time.monotonic() - start) * 1000)

        if ai_result:
            await insight_repo.save_result(student_id, {
                "technical_depth_score":  float(ai_result.get("technical_depth_score", 0)),
                "communication_score":    float(ai_result.get("communication_score", 0)),
                "innovation_score":       float(ai_result.get("innovation_score", 0)),
                "collaboration_score":    float(ai_result.get("collaboration_score", 0)),
                "project_maturity_score": float(ai_result.get("project_maturity_score", 0)),
                "career_readiness_score": float(ai_result.get("career_readiness_score", 0)),
                "ai_summary":             ai_result.get("ai_summary"),
                "strengths":              ai_result.get("strengths"),
                "improvement_areas":      ai_result.get("improvement_areas"),
                "career_fit_roles":       ai_result.get("career_fit_roles"),
                "missing_skills":         ai_result.get("missing_skills"),
                "ai_status":              AIStatus.COMPLETED,
                "model_used":             str(ai_result.get("model_used", "nvidia-api")),
                "processing_time_ms":     elapsed_ms,
                "error_detail":           None,
            })
            logger.info(f"SPICS AI: Analysis complete for student {student_id} in {elapsed_ms}ms")
        else:
            # AI failed — use fallback
            fallback = generate_fallback_summary(
                project_count=len(context.get("projects", [])),
                skill_count=len(context.get("skills", [])),
                cert_count=len(context.get("certifications", [])),
                has_github=bool(context.get("github_username")),
                primary_domain=context.get("primary_domain"),
            )
            await insight_repo.save_result(student_id, {
                **{k: float(v) if isinstance(v, (int, float)) else v for k, v in fallback.items()},
                "ai_status":          AIStatus.DEGRADED,
                "processing_time_ms": elapsed_ms,
                "error_detail":       "AI API unavailable — fallback summary generated",
            })
            logger.info(f"SPICS AI: Used fallback for student {student_id}")

    except Exception as e:
        logger.error(f"SPICS AI: Background worker exception for student {student_id}: {e}", exc_info=True)
        try:
            await insight_repo.save_result(student_id, {
                "ai_status":    AIStatus.FAILED,
                "error_detail": str(e)[:500],
            })
        except Exception:
            pass  # Never propagate worker errors


async def run_resume_ai_analysis(student_id: int, resume_text: str, db: AsyncSession) -> None:
    """Analyzes extracted resume text and attaches insights to the student's AI record."""
    if not FLAGS.get("ENABLE_AI_CAPABILITY_ENGINE", True) or not resume_text:
        return

    from ..ai.capability_analyzer import analyze_resume as _analyze
    insight_repo = InsightRepository(db)
    try:
        result = await _analyze(resume_text)
        if result:
            insight = await insight_repo.get_by_student_id(student_id)
            if insight:
                from ..repositories.data_repos import InsightRepository as IR
                await IR(db).save_result(student_id, {"resume_insights": result})
            else:
                await insight_repo.save_result(student_id, {
                    "resume_insights": result,
                    "ai_status": AIStatus.COMPLETED,
                })
    except Exception as e:
        logger.warning(f"SPICS: Resume AI analysis failed for student {student_id}: {e}")


async def _build_analysis_context(student_id: int, db: AsyncSession) -> dict:
    """Gathers all profile data needed for AI analysis."""
    profile  = await ProfileRepository(db).get_by_student_id(student_id)
    projects = await ProjectRepository(db).list_by_student(student_id)
    certs    = await CertificationRepository(db).list_by_student(student_id)
    skills   = await SkillRepository(db).list_by_student(student_id)

    return {
        "primary_domain":   str(profile.primary_domain or "") if profile else "",
        "bio":              profile.bio if profile else "",
        "github_username":  profile.github_username if profile else None,
        "career_interest":  profile.career_interest if profile else [],
        "projects": [
            {
                "title":       p.title,
                "tech_stack":  p.tech_stack,
                "complexity":  str(p.complexity_level or ""),
                "role":        p.role,
                "team_size":   p.team_size,
                "verified":    str(p.verification_status or "") == "verified",
            }
            for p in projects
        ],
        "skills": [
            {
                "name":        s.skill_name,
                "category":    str(s.category or ""),
                "proficiency": str(s.proficiency_level or ""),
                "self_rating": s.self_rating,
            }
            for s in skills
        ],
        "certifications": [
            {"title": c.title, "provider": c.provider}
            for c in certs
        ],
    }
