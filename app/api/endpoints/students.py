from __future__ import annotations
from typing import List, Optional
import logging
from fastapi import APIRouter, Depends, HTTPException, Query, Path, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...core import auth
from ...core.database import get_db
from ...models import base as models
from ...schemas import base as schemas
from ...core.limiter import limiter
from ...services.student_service import StudentService
from ...services.intelligence_engine import StudentIntelligenceEngine
from ...services.timetable_service import get_section_timetable
from ...professional_identity.models.models import AIProfessionalInsight

logger = logging.getLogger(__name__)

# Common responses for students router
STUDENT_RESPONSES = {
    401: {"description": "Unauthorized - Missing or invalid token", "model": schemas.MessageResponse},
    404: {"description": "Student not found", "model": schemas.MessageResponse},
}

router = APIRouter(tags=["Students"], responses=STUDENT_RESPONSES)

@router.get(
    "/timetable",
    response_model=List[schemas.StaffTimeTableEntry],
    summary="Get Timetable for Student",
    description="Returns weekly timetable for the student's section with fallback static data for MCA II semester.",
)
async def get_student_timetable(
    request: Request,
    section: Optional[str] = Query(None, description="Override section, defaults to student's section"),
    semester: Optional[int] = Query(None, description="Override semester, defaults to student's current semester"),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    # Determine the caller's student profile (if present) to derive defaults
    student = None
    if current_user.role.name == "student":
        result = await db.execute(select(models.Student).filter(models.Student.id == current_user.id))
        student = result.scalars().first()
        if not student:
            raise HTTPException(status_code=404, detail="Student record not found")

    # Fix section detection logic - student.section can be NULL/empty
    if section:
        derived_section = section
        logger.info(f"Using query parameter section: {derived_section}")
    elif student and student.section:
        derived_section = student.section
        logger.info(f"Student {student.id} using database section: {derived_section}")
    else:
        derived_section = "A"  # Fallback only when no section available
        logger.warning(f"Student {student.id if student else 'None'} has no section, defaulting to A")
    
    derived_semester = semester or (student.current_semester if student else None) or 2
    logger.info(f"Student timetable request: section={derived_section}, semester={derived_semester}")

    timetable = await get_section_timetable(db=db, section=derived_section, semester=derived_semester)
    return timetable

@router.get(
    "/performance/{roll_no}", 
    response_model=schemas.StudentPerformance,
    summary="Get Student Performance",
    description="Retrieve comprehensive academic performance record for a specific student including SGPA trends and subject-wise grades."
)
@limiter.limit("20/minute")
async def get_student_performance(
    request: Request,
    roll_no: str = Path(..., description="Student roll number"),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Get detailed academic performance record for a specific student.
    """
    student = await StudentService.get_accessible_student(
        roll_no,
        current_user.id,
        current_user.role.name if current_user.role else "student",
        db,
    )
    marks = await StudentService.get_report_card_marks(student.id, db)

    analytics = await StudentService.calculate_analytics(student, db)
    payload = schemas.StudentPerformance.model_validate(student, from_attributes=True)
    payload.marks = marks
    payload.semester_performance = analytics.semester_performance
    return payload

@router.get(
    "/analytics/{roll_no}", 
    response_model=schemas.AnalyticsSummary,
    summary="Get Student Analytics",
    description="Retrieve processed academic insights, percentile rankings, and skill domain mapping for a student."
)
@limiter.limit("20/minute")
async def get_student_analytics(
    request: Request,
    roll_no: str = Path(..., description="Student roll number"),
    semester: Optional[int] = Query(None, description="Select semester for analytics"),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Get consolidated academic analytics for a student.
    """
    student = await StudentService.get_accessible_student(roll_no, current_user.id, current_user.role.name if current_user.role else "student", db)
    return await StudentService.calculate_analytics(student, db, semester)

@router.get(
    "/command-center/{roll_no}", 
    response_model=schemas.StudentCommandCenterResponse,
    summary="Get Student Command Center",
    description="Retrieve a high-level executive dashboard for a student, including core metrics, risk indicators, and peer benchmarks."
)
@limiter.limit("20/minute")
async def get_student_command_center(
    request: Request,
    roll_no: str = Path(..., description="Student roll number"),
    semester: Optional[int] = Query(None, description="Select semester for dashboard metrics"),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Get a high-level overview of a student's standing, metrics, and risk status.
    """
    student = await StudentService.get_accessible_student(roll_no, current_user.id, current_user.role.name if current_user.role else "student", db)
    return await StudentService.build_student_command_center(student, db, semester)
@router.get(
    "/attendance/{roll_no}", 
    response_model=schemas.PaginatedAttendance,
    summary="Get Detailed Attendance",
    description="Retrieve paginated daily attendance records for a student, with optional semester filtering."
)
async def get_student_attendance(
    roll_no: str = Path(..., description="Student roll number"),
    semester: int | None = Query(None, description="Filter by semester"),
    page: int = Query(1, ge=1, description="Page number"),
    size: int = Query(20, ge=1, le=100, description="Records per page"),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Get detailed paginated attendance for a student.
    """
    student = await StudentService.get_accessible_student(roll_no, current_user.id, current_user.role.name if current_user.role else "student", db)
    return await StudentService.get_detailed_attendance(student.id, semester, page, size, db)

@router.get(
    "/dna/{roll_no}",
    response_model=schemas.StudentDNAResponse,
    summary="Get Student Capability DNA",
    description="Retrieve capability scores and AI-generated intelligence profile for a student."
)
@limiter.limit("15/minute")
async def get_student_dna(
    request: Request,
    roll_no: str = Path(..., description="Student roll number"),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    student = await StudentService.get_accessible_student(roll_no, current_user.id, current_user.role.name if current_user.role else "student", db)
    
    cap_score = await db.get(models.StudentCapabilityScore, student.id)
    ai_profile_res = await db.execute(
        select(AIProfessionalInsight).where(AIProfessionalInsight.student_id == student.id)
    )
    ai_profile = ai_profile_res.scalars().first()
    
    if not cap_score or not ai_profile:
        # Trigger dynamic pre-calculation and save
        return await StudentIntelligenceEngine.analyze_and_cache_student(student.id, db)
    
    cap_score_dto = schemas.StudentCapabilityScoreResponse.model_validate(cap_score)
    ai_profile_dto = None
    if ai_profile:
        ai_profile_dto = schemas.StudentAIProfileResponse(
            primary_identity=f"Promising {cap_score.profile_type or 'Balanced Performer'}",
            secondary_identity="Active Learner",
            strengths=ai_profile.strengths or [],
            weaknesses=ai_profile.improvement_areas or [],
            recommendations=ai_profile.missing_skills or [],
            placement_probability=float(cap_score.placement_probability or 0.0),
            career_fit=[schemas.CareerFitItem(**item) for item in (ai_profile.career_fit_roles or [])],
            ai_summary=ai_profile.ai_summary,
            confidence_score=float(cap_score.confidence_score or 0.9),
            generated_at=ai_profile.generated_at
        )
        
    return schemas.StudentDNAResponse(
        roll_no=student.roll_no,
        student_name=student.name,
        capability_scores=cap_score_dto,
        ai_profile=ai_profile_dto,
        spi_score=float(cap_score.spi_score),
        profile_type=cap_score.profile_type or "Balanced Performer"
    )

@router.get(
    "/career-fit/{roll_no}",
    response_model=schemas.CareerFitResponse,
    summary="Get Student Career Fit Analysis",
    description="Retrieve predicted suitable career domains, match percentages, and AI explanations."
)
@limiter.limit("15/minute")
async def get_student_career_fit(
    request: Request,
    roll_no: str = Path(..., description="Student roll number"),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    student = await StudentService.get_accessible_student(roll_no, current_user.id, current_user.role.name if current_user.role else "student", db)
    
    ai_profile_res = await db.execute(
        select(AIProfessionalInsight).where(AIProfessionalInsight.student_id == student.id)
    )
    ai_profile = ai_profile_res.scalars().first()
    cap_score = await db.get(models.StudentCapabilityScore, student.id)
    if not ai_profile or not cap_score:
        # Trigger generation
        dna = await StudentIntelligenceEngine.analyze_and_cache_student(student.id, db)
        ai_profile_dto = dna.ai_profile
    else:
        ai_profile_dto = schemas.StudentAIProfileResponse(
            primary_identity=f"Promising {cap_score.profile_type or 'Balanced Performer'}",
            secondary_identity="Active Learner",
            strengths=ai_profile.strengths or [],
            weaknesses=ai_profile.improvement_areas or [],
            recommendations=ai_profile.missing_skills or [],
            placement_probability=float(cap_score.placement_probability or 0.0),
            career_fit=[schemas.CareerFitItem(**item) for item in (ai_profile.career_fit_roles or [])],
            ai_summary=ai_profile.ai_summary,
            confidence_score=float(cap_score.confidence_score or 0.9),
            generated_at=ai_profile.generated_at
        )
        
    fits = ai_profile_dto.career_fit if ai_profile_dto else []
    explanation = ai_profile_dto.ai_summary if ai_profile_dto else "Calculated based on skill domain matching."
    
    return schemas.CareerFitResponse(
        roll_no=student.roll_no,
        student_name=student.name,
        career_fit=fits,
        ai_explanation=explanation
    )

@router.get(
    "/skill-radar/{roll_no}",
    response_model=schemas.StudentCapabilityScoreResponse,
    summary="Get Student Skill Radar",
    description="Retrieve 10-dimensional capability scores for radar chart visualization."
)
@limiter.limit("20/minute")
async def get_student_skill_radar(
    request: Request,
    roll_no: str = Path(..., description="Student roll number"),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    student = await StudentService.get_accessible_student(roll_no, current_user.id, current_user.role.name if current_user.role else "student", db)
    
    cap_score = await db.get(models.StudentCapabilityScore, student.id)
    if not cap_score:
        dna = await StudentIntelligenceEngine.analyze_and_cache_student(student.id, db)
        return dna.capability_scores
        
    return schemas.StudentCapabilityScoreResponse.model_validate(cap_score)

@router.get(
    "/potential-index/{roll_no}",
    response_model=schemas.PotentialIndexResponse,
    summary="Get Student Potential Index",
    description="Retrieve SPI, growth trend across semesters, and peer cohort percentile ranking."
)
@limiter.limit("20/minute")
async def get_student_potential_index(
    request: Request,
    roll_no: str = Path(..., description="Student roll number"),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    student = await StudentService.get_accessible_student(roll_no, current_user.id, current_user.role.name if current_user.role else "student", db)
    
    cap_score = await db.get(models.StudentCapabilityScore, student.id)
    if not cap_score:
        dna = await StudentIntelligenceEngine.analyze_and_cache_student(student.id, db)
        cap_score_dto = dna.capability_scores
    else:
        cap_score_dto = schemas.StudentCapabilityScoreResponse.model_validate(cap_score)
        
    # Fetch growth history
    growth_history_res = await db.execute(
        select(models.StudentGrowthHistory)
        .filter(models.StudentGrowthHistory.student_id == student.id)
        .order_by(models.StudentGrowthHistory.semester.asc())
    )
    growth_history = growth_history_res.scalars().all()
    growth_history_dto = [schemas.StudentGrowthHistoryResponse.model_validate(h) for h in growth_history]
    
    # Calculate peer percentile based on SPI score
    total_students = await db.scalar(select(func.count(models.Student.id)))
    lower_spi_count = await db.scalar(
        select(func.count(models.StudentCapabilityScore.student_id))
        .filter(models.StudentCapabilityScore.spi_score < cap_score_dto.spi_score)
    )
    
    peer_percentile = (lower_spi_count / total_students * 100.0) if total_students > 0 else 100.0
    
    status_label = "Consistent"
    if len(growth_history_dto) >= 2:
        delta = growth_history_dto[-1].growth_delta
        if delta > 3.0:
            status_label = "Rising"
        elif delta < -3.0:
            status_label = "Intervention Needed"
            
    return schemas.PotentialIndexResponse(
        roll_no=student.roll_no,
        student_name=student.name,
        current_spi=float(cap_score_dto.spi_score),
        growth_history=growth_history_dto,
        peer_percentile=round(peer_percentile, 1),
        status_label=status_label
    )

@router.get(
    "/ai-summary/{roll_no}",
    response_model=schemas.AISummaryResponse,
    summary="Get AI Summary and Recommendations",
    description="Retrieve identities, qualitative strengths, weaknesses, and growth-oriented recommendations."
)
@limiter.limit("15/minute")
async def get_student_ai_summary(
    request: Request,
    roll_no: str = Path(..., description="Student roll number"),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    student = await StudentService.get_accessible_student(roll_no, current_user.id, current_user.role.name if current_user.role else "student", db)
    
    ai_profile_res = await db.execute(
        select(AIProfessionalInsight).where(AIProfessionalInsight.student_id == student.id)
    )
    ai_profile = ai_profile_res.scalars().first()
    cap_score = await db.get(models.StudentCapabilityScore, student.id)
    
    if not ai_profile or not cap_score:
        dna = await StudentIntelligenceEngine.analyze_and_cache_student(student.id, db)
        ai_profile_dto = dna.ai_profile
        profile_type = dna.profile_type
        conf_score = 0.90
    else:
        profile_type = cap_score.profile_type or "Balanced Performer"
        conf_score = float(cap_score.confidence_score or 0.90)
        ai_profile_dto = schemas.StudentAIProfileResponse(
            primary_identity=f"Promising {profile_type}",
            secondary_identity="Active Learner",
            strengths=ai_profile.strengths or [],
            weaknesses=ai_profile.improvement_areas or [],
            recommendations=ai_profile.missing_skills or [],
            placement_probability=float(cap_score.placement_probability or 0.0),
            career_fit=[schemas.CareerFitItem(**item) for item in (ai_profile.career_fit_roles or [])],
            ai_summary=ai_profile.ai_summary,
            confidence_score=conf_score,
            generated_at=ai_profile.generated_at
        )
        
    return schemas.AISummaryResponse(
        roll_no=student.roll_no,
        student_name=student.name,
        primary_identity=ai_profile_dto.primary_identity or f"Promising {profile_type}",
        secondary_identity=ai_profile_dto.secondary_identity or "Active Learner",
        strengths=ai_profile_dto.strengths or [],
        weaknesses=ai_profile_dto.weaknesses or [],
        recommendations=ai_profile_dto.recommendations or [],
        ai_summary=ai_profile_dto.ai_summary or "No summary generated.",
        confidence_score=conf_score
    )
