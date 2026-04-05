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
from ...services.timetable_service import get_section_timetable

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

    payload = schemas.StudentPerformance.model_validate(student, from_attributes=True)
    payload.marks = marks
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
