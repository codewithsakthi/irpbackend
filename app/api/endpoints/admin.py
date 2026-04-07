from enum import Enum
from typing import Optional, List
from fastapi import APIRouter, Depends, Query, Path, HTTPException, Response, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from ...core import auth
from ...core.database import get_db, settings
from ...core.constants import CURRICULUM_CREDITS, GRADE_POINTS
from ...services.student_service import StudentService
from ...models import base as models
from ...schemas import base as schemas
from ...services.admin_service import AdminService
from ...core.limiter import limiter
from ...services import enterprise_analytics
from sqlalchemy import select, update, delete, func, case as sql_case, and_

router = APIRouter(tags=["Admin"])

@router.patch("/password", response_model=schemas.MessageResponse)
async def change_admin_password(
    payload: schemas.PasswordChangeRequest,
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Allow an authenticated admin to change their own password.
    """
    # Only allow admins
    if not hasattr(current_user, "role_id") or getattr(current_user, "role_id", None) != 1:
        raise HTTPException(status_code=403, detail="Only admins can change their password here.")
    if not auth.verify_password(payload.current_password, current_user.password_hash):
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    if len(payload.new_password) < 6:
        raise HTTPException(status_code=422, detail="New password must be at least 6 characters long")
    if payload.current_password == payload.new_password:
        raise HTTPException(status_code=422, detail="New password must be different from the current password")
    current_user.password_hash = auth.get_password_hash(payload.new_password)
    current_user.is_initial_password = False
    await db.commit()
    return schemas.MessageResponse(message="Password updated successfully")

# Enum Definitions for API Constraints
class RiskLevel(str, Enum):
    CRITICAL = "Critical"
    HIGH = "High"
    MODERATE = "Moderate"
    LOW = "Low"

class BottleneckSortBy(str, Enum):
    FAILURE_RATE = "failure_rate"
    AVG_GRADE = "avg_grade"
    STUDENT_COUNT = "student_count"

class FacultySortBy(str, Enum):
    FAILURE_RATE = "failure_rate"
    AVERAGE_MARKS = "average_marks"
    STUDENT_COUNT = "student_count"

class ReadinessSortBy(str, Enum):
    CGPA = "cgpa"
    ATTENDANCE = "attendance"
    CODING_SCORE = "coding_score"

class RiskSortBy(str, Enum):
    RISK_SCORE = "risk_score"
    GPA_DROP = "gpa_drop"
    ATTENDANCE = "attendance"

class StudentSortBy(str, Enum):
    ROLL_NO = "roll_no"
    NAME = "name"
    GPA = "gpa"
    ATTENDANCE = "attendance"
    RANK = "rank"
    BACKLOGS = "backlogs"

class SortDir(str, Enum):
    ASC = "asc"
    DESC = "desc"

# Common responses for Admin router
ADMIN_RESPONSES = {
    401: {"description": "Unauthorized - Missing or invalid token", "model": schemas.MessageResponse},
    403: {"description": "Forbidden - Admin access required", "model": schemas.MessageResponse},
    404: {"description": "Resource not found", "model": schemas.MessageResponse},
}

router = APIRouter(tags=["Admin"], responses=ADMIN_RESPONSES)

def require_admin(user: models.User):
    if not user.role or user.role.name != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

@router.get(
    "/overview", 
    response_model=schemas.AdminOverview,
    summary="Get Administrative Overview",
    description="Retrieve high-level statistics for the admin dashboard, including student, staff, and admin counts."
)
async def get_admin_overview(
    current_user: models.User = Depends(auth.get_current_user), 
    db: AsyncSession = Depends(get_db),
    batch: Optional[str] = Query(default=None)
):
    require_admin(current_user)
    credits_values = ", ".join(f"('{code}', {credit})" for code, credit in CURRICULUM_CREDITS.items())
    directory = await AdminService.build_admin_directory(db, credits_values)
    if batch and batch.upper() != 'ALL':
        directory = [d for d in directory if (d.batch or '').upper() == batch.upper()]
    
    staff_count_res = await db.execute(select(func.count(models.Staff.id)))
    staff_count = staff_count_res.scalar() or 0

    return schemas.AdminOverview(
        total_students=len(directory),
        total_staff=staff_count,
        total_admins=1,
    )

@router.get(
    "/command-center", 
    response_model=schemas.AdminCommandCenterResponse,
    summary="Get Admin Command Center",
    description="Retrieve an executive real-time dashboard for the entire institution, featuring department health, risk summaries, and spotlight insights."
)
async def get_command_center(
    spotlight: str = "",
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    require_admin(current_user)
    return await enterprise_analytics.get_command_center(db, CURRICULUM_CREDITS, spotlight=spotlight)

@router.get(
    "/batches",
    response_model=List[str],
    summary="Get Unique Batches",
    description="Retrieve a list of all unique student batches currently in the database."
)
async def get_batches(
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    require_admin(current_user)
    result = await db.execute(select(models.Student.batch).distinct().order_by(models.Student.batch.desc()))
    return [row for row in result.scalars().all() if row]

@router.get(
    "/student-360/{roll_no}", 
    response_model=schemas.Student360Profile,
    summary="Get Student 360 View",
    description="Retrieve a complete holistic profile of a specific student, including academic history, risk factors, and behavioral insights.",
    tags=["Analytics", "Student Intelligence"],
)
async def get_student_360(
    roll_no: str = Path(
        ..., 
        description="Student roll number",
        min_length=1,
        max_length=20,
        pattern="^[A-Za-z0-9]+$"
    ),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get a comprehensive 360-degree view of a student including:
    - Academic metrics (GPA, attendance, trend analysis)
    - Risk assessment (velocity, backlog, correlation factors)
    - Peer benchmarking (rank, percentile, cohort comparison)
    - Subject strengths and support areas
    - Personalized recommendations

    **Required Role**: Admin

    **Response includes**:
    - Student profile (basic info & current metrics)
    - GPA velocity trend (Rising/Stable/Falling)
    - Risk drivers (Attendance, Internals, GPA Velocity, Backlog Load)
    - Skill domain scores
    - Semester-wise velocity
    - Subject highlights (strengths & support needed)
    - Peer benchmark (class rank, percentile, gap from cohort avg)
    - AI-generated recommended actions
    """
    require_admin(current_user)
    
    # Input validation
    if not roll_no or not roll_no.strip():
        raise HTTPException(status_code=400, detail="Roll number cannot be empty")
    
    roll_no = roll_no.strip().upper()
    
    try:
        profile = await enterprise_analytics.get_student_360(
            db, 
            CURRICULUM_CREDITS, 
            roll_no=roll_no
        )
        return profile
    except HTTPException as e:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        # Log unexpected errors
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Error fetching Student 360 profile for {roll_no}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to generate Student 360 profile: {str(e)}"
        )

@router.get(
    "/student-360-metrics",
    summary="Student 360 Module Metrics",
    description="Get performance metrics for Student 360 profile generation (cache hit rate, response times, etc.)",
    tags=["Analytics", "Monitoring"],
)
async def get_student_360_metrics(
    current_user: models.User = Depends(auth.get_current_user),
):
    """
    Returns performance metrics for the Student 360 module including:
    - Request counts (total, success, failed)
    - Average response time
    - Cache performance (hit rate)
    """
    require_admin(current_user)
    
    from ...services.student_360_utils import get_student_360_metrics
    return get_student_360_metrics()

@router.get(
    "/bottlenecks", 
    response_model=schemas.SubjectBottleneckResponse,
    summary="Get Academic Bottlenecks",
    description="Identify subjects with high failure rates or significant performance anomalies across the batch."
)
async def get_subject_bottlenecks(
    sort_by: BottleneckSortBy = Query(default=BottleneckSortBy.AVG_GRADE, description="Field to sort by"),
    sort_dir: SortDir = Query(default=SortDir.ASC, description="Sort direction (asc/desc)"),
    limit: int = Query(default=20, ge=1, le=100, description="Items per page"),
    offset: int = Query(default=0, ge=0, description="Items to skip"),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Identify subjects with high failure rates or low average grades.
    """
    require_admin(current_user)
    return await enterprise_analytics.get_subject_bottlenecks(db, CURRICULUM_CREDITS, subject_code=None, limit=limit, offset=offset, sort_by=sort_by.value)

@router.get("/subject-catalog", response_model=list[schemas.SubjectCatalogItem])
async def get_subject_catalog(
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    require_admin(current_user)
    return await enterprise_analytics.get_subject_catalog(db)

# Original /subject-bottlenecks endpoint removed as per instruction to replace with /bottlenecks
# @router.get("/subject-bottlenecks", response_model=schemas.SubjectBottleneckResponse)
# async def get_subject_bottlenecks(
#     subject_code: Optional[str] = None,
#     limit: int = 10,
#     offset: int = 0,
#     sort_by: BottleneckSortBy = BottleneckSortBy.FAILURE_RATE,
#     current_user: models.User = Depends(auth.get_current_user),
#     db: AsyncSession = Depends(get_db)
# ):
#     require_admin(current_user)
#     return await enterprise_analytics.get_subject_bottlenecks(db, CURRICULUM_CREDITS, subject_code=subject_code, limit=limit, offset=offset, sort_by=sort_by.value)

@router.get(
    "/impact-matrix", 
    response_model=schemas.FacultyImpactMatrixResponse,
    summary="Get Faculty Impact Matrix",
    description="Analyze faculty effectiveness across different subjects based on student pass rates and average performance."
)
async def get_impact_matrix(
    sort_by: FacultySortBy = Query(default=FacultySortBy.FAILURE_RATE, description="Field to sort by"),
    sort_dir: SortDir = Query(default=SortDir.DESC, description="Sort direction (asc/desc)"),
    limit: int = Query(default=20, ge=1, le=100, description="Items per page"),
    offset: int = Query(default=0, ge=0, description="Items to skip"),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get faculty impact matrix analyzing performance across subjects.
    """
    require_admin(current_user)
    return await enterprise_analytics.get_faculty_impact_matrix(db, CURRICULUM_CREDITS, subject_code=None, faculty_id=None, limit=limit, offset=offset)

@router.get("/placement-readiness", response_model=schemas.PlacementReadinessResponse)
async def get_placement_readiness(
    sort_by: ReadinessSortBy = Query(default=ReadinessSortBy.CGPA, description="Field to sort by"),
    sort_dir: SortDir = Query(default=SortDir.DESC, description="Sort direction (asc/desc)"),
    limit: int = Query(default=20, ge=1, le=100, description="Items per page"),
    offset: int = Query(default=0, ge=0, description="Items to skip"),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get students ranked by their placement readiness and technical scores.
    """
    require_admin(current_user)
    cgpa_threshold: float = 6.5 
    return await enterprise_analytics.get_placement_readiness(db, CURRICULUM_CREDITS, cgpa_threshold=cgpa_threshold, limit=limit, offset=offset, sort_by=sort_by.value)

@router.get(
    "/risk/registry", 
    response_model=schemas.RiskRegistryResponse,
    summary="Get Batch Risk Registry",
    description="Identify and rank students at high academic risk across the entire institution for proactive intervention."
)
async def get_risk_registry(
    risk_level: Optional[RiskLevel] = Query(default=None, description="Filter by risk level"),
    sort_by: RiskSortBy = Query(default=RiskSortBy.RISK_SCORE, description="Field to sort by"),
    sort_dir: SortDir = Query(default=SortDir.DESC, description="Sort direction (asc/desc)"),
    limit: int = Query(default=20, ge=1, le=100, description="Items per page"),
    offset: int = Query(default=0, ge=0, description="Items to skip"),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get a list of at-risk students based on attendance and performance.
    """
    require_admin(current_user)
    return await enterprise_analytics.get_risk_registry(db, CURRICULUM_CREDITS, risk_level=risk_level.value if risk_level else None, limit=limit, offset=offset, sort_by=sort_by.value)

@router.get(
    "/staff",
    response_model=list[schemas.StaffProfile],
    summary="List Staff",
    description="Get all staff profiles with usernames and departments.",
)
async def list_staff(
    search: str = Query(default="", description="Optional search across name, username, email, department"),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    require_admin(current_user)
    # Get staff with their assigned subjects
    stmt = (
        select(
            models.Staff, 
            models.User,
            func.array_agg(models.Subject.course_code).label('subject_codes'),
            func.array_agg(models.Subject.name).label('subject_names')
        )
        .join(models.User, models.Staff.id == models.User.id)
        .outerjoin(models.FacultySubjectAssignment, models.Staff.id == models.FacultySubjectAssignment.faculty_id)
        .outerjoin(models.Subject, models.FacultySubjectAssignment.subject_id == models.Subject.id)
        .group_by(models.Staff.id, models.User.id)
        .order_by(models.Staff.name)
    )
    result = await db.execute(stmt)
    rows = result.all()
    profiles: list[schemas.StaffProfile] = []
    for staff, user, subject_codes, subject_names in rows:
        # Filter out None values from arrays (when no subjects assigned)
        subjects = []
        if subject_codes and subject_codes[0] is not None:
            subjects = [
                {"code": code, "name": name} 
                for code, name in zip(subject_codes, subject_names)
                if code is not None
            ]
        
        blob = {
            "id": staff.id,
            "username": user.username,
            "name": staff.name,
            "email": staff.email,
            "department": staff.department,
            "created_at": staff.created_at,
            "subjects": subjects,  # Add subjects to response
        }
        if search:
            s = search.lower()
            # Check if search term is found in any of the searchable fields (OR logic)
            name_match = s in (staff.name or "").lower()
            username_match = s in (user.username or "").lower()
            email_match = s in (staff.email or "").lower()
            department_match = s in (staff.department or "").lower()
            
            # Skip if search term is not found in any field
            if not (name_match or username_match or email_match or department_match):
                continue
        profiles.append(schemas.StaffProfile(**blob))
    return profiles


@router.post(
    "/staff",
    response_model=schemas.StaffProfile,
    summary="Create Staff User",
    description="Add a new staff user with login credentials and profile details.",
)
async def create_staff(
    payload: schemas.StaffCreate,
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    require_admin(current_user)

    # Validate username uniqueness
    existing_user = await db.execute(select(models.User).filter(models.User.username == payload.username))
    if existing_user.scalars().first():
        raise HTTPException(status_code=400, detail="Username already exists")

    # Validate email uniqueness if provided
    if payload.email:
        existing_email = await db.execute(select(models.Staff).filter(models.Staff.email == payload.email))
        if existing_email.scalars().first():
            raise HTTPException(status_code=400, detail="Email already exists")

    # Resolve staff role
    role_res = await db.execute(select(models.Role).filter(models.Role.name == "staff"))
    staff_role = role_res.scalars().first()
    if not staff_role:
        raise HTTPException(status_code=400, detail="Staff role not configured")

    hashed_pwd = auth.get_password_hash(payload.password)

    user = models.User(
        username=payload.username,
        password_hash=hashed_pwd,
        role_id=staff_role.id,
        is_initial_password=True,
    )
    db.add(user)
    await db.flush()

    staff = models.Staff(
        id=user.id,
        name=payload.name,
        email=payload.email,
        department=payload.department,
    )
    db.add(staff)
    await db.commit()
    await db.refresh(user)
    await db.refresh(staff)

    return schemas.StaffProfile(
        id=staff.id,
        username=user.username,
        name=staff.name,
        email=staff.email,
        department=staff.department,
        created_at=staff.created_at,
    )


@router.patch(
    "/staff/{staff_id}",
    response_model=schemas.StaffProfile,
    summary="Update Staff User",
    description="Edit staff profile or reset password.",
)
async def update_staff(
    staff_id: int = Path(..., ge=1),
    payload: schemas.StaffUpdate = None,
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    require_admin(current_user)
    if payload is None:
        raise HTTPException(status_code=400, detail="No update data provided")

    staff_res = await db.execute(
        select(models.Staff, models.User).join(models.User, models.Staff.id == models.User.id).filter(models.Staff.id == staff_id)
    )
    row = staff_res.first()
    if not row:
        raise HTTPException(status_code=404, detail="Staff not found")
    staff, user = row

    # Validate email uniqueness if being updated
    if payload.email and payload.email != staff.email:
        existing_email = await db.execute(
            select(models.Staff).filter(models.Staff.email == payload.email, models.Staff.id != staff_id)
        )
        if existing_email.scalars().first():
            raise HTTPException(status_code=400, detail="Email already exists")

    # Update basic fields
    if payload.name is not None:
        staff.name = payload.name
    if payload.email is not None:
        staff.email = payload.email
    if payload.department is not None:
        staff.department = payload.department
    if payload.password:
        user.password_hash = auth.get_password_hash(payload.password)
        user.is_initial_password = True

    await db.commit()
    await db.refresh(staff)
    await db.refresh(user)

    return schemas.StaffProfile(
        id=staff.id,
        username=user.username,
        name=staff.name,
        email=staff.email,
        department=staff.department,
        created_at=staff.created_at,
    )

@router.delete(
    "/staff/{staff_id}",
    status_code=204,
    summary="Delete Staff User",
    description="Remove a staff account and related timetable/assignment links.",
)
async def delete_staff(
    staff_id: int = Path(..., ge=1),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    require_admin(current_user)

    # Ensure the staff exists and load matching user
    staff_res = await db.execute(
        select(models.Staff, models.User)
        .join(models.User, models.Staff.id == models.User.id)
        .filter(models.Staff.id == staff_id)
    )
    row = staff_res.first()
    if not row:
        raise HTTPException(status_code=404, detail="Staff not found")

    # Remove dependent records first (no ON DELETE CASCADE defined)
    await db.execute(delete(models.TimeTable).where(models.TimeTable.faculty_id == staff_id))
    await db.execute(delete(models.FacultySubjectAssignment).where(models.FacultySubjectAssignment.faculty_id == staff_id))
    await db.execute(delete(models.RefreshToken).where(models.RefreshToken.user_id == staff_id))

    # Remove staff and linked user
    await db.execute(delete(models.Staff).where(models.Staff.id == staff_id))
    await db.execute(delete(models.User).where(models.User.id == staff_id))

    await db.commit()
    return Response(status_code=204)


@router.get(
    "/staff/{staff_id}/subjects",
    response_model=schemas.StaffSubjectAssign,
    summary="List subjects assigned to staff",
)
async def get_staff_subjects(
    staff_id: int = Path(..., ge=1),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    require_admin(current_user)
    rows = await db.execute(
        select(models.Subject.id, models.Subject.course_code)
        .join(models.FacultySubjectAssignment, models.FacultySubjectAssignment.subject_id == models.Subject.id)
        .where(models.FacultySubjectAssignment.faculty_id == staff_id)
    )
    subject_ids: list[int] = []
    subject_codes: list[str] = []
    for sid, code in rows.all():
        if sid is not None:
            subject_ids.append(int(sid))
        if code:
            subject_codes.append(code)
    # Deduplicate while preserving order
    def dedup(seq):
        seen = set()
        out = []
        for item in seq:
            if item in seen:
                continue
            seen.add(item)
            out.append(item)
        return out

    return schemas.StaffSubjectAssign(subject_ids=dedup(subject_ids), subject_codes=dedup(subject_codes))


@router.post(
    "/staff/{staff_id}/subjects",
    response_model=schemas.MessageResponse,
    summary="Assign subjects to staff",
    description="Replace the staff member's subject assignments with the provided list of subject IDs.",
)
async def assign_staff_subjects(
    staff_id: int = Path(..., ge=1),
    payload: schemas.StaffSubjectAssign = None,
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    require_admin(current_user)
    payload = payload or schemas.StaffSubjectAssign(subject_ids=[])

    staff_exists = await db.scalar(
        select(func.count()).select_from(models.Staff).filter(models.Staff.id == staff_id)
    )
    if not staff_exists:
        raise HTTPException(status_code=404, detail="Staff not found")

    subject_ids = list(payload.subject_ids or [])
    if payload.subject_codes:
        # Remove duplicates from input codes
        unique_codes = list(set(payload.subject_codes))
        rows = await db.execute(
            select(models.Subject.id, models.Subject.course_code).filter(models.Subject.course_code.in_(unique_codes))
        )
        found = rows.all()
        subject_ids.extend([r.id for r in found])
        # Check if all codes were found
        if len(found) != len(unique_codes):
            found_codes = {r.course_code for r in found}
            missing_codes = set(unique_codes) - found_codes
            raise HTTPException(status_code=400, detail=f"Subject codes not found: {', '.join(missing_codes)}")

    # Remove duplicate subject IDs 
    subject_ids = list(set(subject_ids))

    if subject_ids:
        valid_count = await db.scalar(
            select(func.count()).select_from(models.Subject).filter(models.Subject.id.in_(subject_ids))
        )
        if valid_count != len(subject_ids):
            raise HTTPException(status_code=400, detail="One or more subject IDs are invalid")

    # Replace assignments
    await db.execute(
        delete(models.FacultySubjectAssignment).where(models.FacultySubjectAssignment.faculty_id == staff_id)
    )
    for sid in subject_ids:
        db.add(models.FacultySubjectAssignment(faculty_id=staff_id, subject_id=sid))

    await db.commit()
    return schemas.MessageResponse(message="Subjects assigned successfully")


@router.get(
    "/subject-catalog",
    response_model=List[schemas.SubjectCatalogItem],
    summary="Get Subject Catalog",
    description="Get all available subjects for assignment to staff members. Optionally filter by batch and section.",
)
async def get_subject_catalog(
    batch: Optional[str] = Query(None, description="Filter subjects by student batch (e.g., '2021-25')"),
    section: Optional[str] = Query(None, description="Filter subjects by section (e.g., 'A', 'B')"),
    semester: Optional[int] = Query(None, description="Filter subjects by semester"),
    program_id: Optional[int] = Query(None, description="Filter subjects by program"),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    require_admin(current_user)
    
    # Build base query for subjects with record count
    stmt = (
        select(
            models.Subject.id,
            models.Subject.course_code,
            models.Subject.name,
            models.Subject.semester,
            models.Subject.is_active,
            models.Subject.program_id,
            func.count(models.StudentAssessment.id).label('records')
        )
        .outerjoin(models.StudentAssessment, models.Subject.id == models.StudentAssessment.subject_id)
    )
    
    # Apply filters
    if semester:
        stmt = stmt.filter(models.Subject.semester == semester)
    if program_id:
        stmt = stmt.filter(models.Subject.program_id == program_id)
        
    # If batch or section is provided, we need to join with students 
    # and faculty assignments to filter subjects that are actually taught
    # to students in that batch/section
    if batch or section:
        # Join with FacultySubjectAssignment to get section assignments
        stmt = stmt.join(
            models.FacultySubjectAssignment, 
            models.Subject.id == models.FacultySubjectAssignment.subject_id
        )
        if section:
            stmt = stmt.filter(models.FacultySubjectAssignment.section == section)
            
        # If batch is specified, ensure the subject is for students in that batch
        if batch:
            # Join with Students to check batch
            stmt = stmt.join(
                models.Student, 
                and_(
                    models.Student.program_id == models.Subject.program_id,
                    models.Student.current_semester == models.Subject.semester,
                    models.Student.section == models.FacultySubjectAssignment.section if section else True
                )
            ).filter(models.Student.batch == batch)
    
    # Group and order
    stmt = stmt.group_by(
        models.Subject.id, 
        models.Subject.course_code, 
        models.Subject.name, 
        models.Subject.semester, 
        models.Subject.is_active,
        models.Subject.program_id
    ).order_by(models.Subject.semester, models.Subject.course_code)
    
    result = await db.execute(stmt)
    subjects_with_counts = result.all()
    
    return [
        schemas.SubjectCatalogItem(
            id=row.id,
            subject_code=row.course_code,
            subject_name=row.name,
            semester=row.semester,
            records=row.records or 0,
            is_active=row.is_active,
        )
        for row in subjects_with_counts
    ]


@router.patch(
    "/subjects/{subject_id}/toggle",
    response_model=schemas.SubjectToggleResponse,
    summary="Toggle Subject Active Status",
    description="Enable or disable a subject for the current semester. Inactive subjects will be hidden from staff and student interfaces.",
)
async def toggle_subject_status(
    subject_id: int,
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    require_admin(current_user)
    
    # Find the subject
    stmt = select(models.Subject).where(models.Subject.id == subject_id)
    result = await db.execute(stmt)
    subject = result.scalar_one_or_none()
    
    if not subject:
        raise HTTPException(
            status_code=404,
            detail="Subject not found"
        )
    
    # Toggle the active status
    subject.is_active = not subject.is_active
    await db.commit()
    await db.refresh(subject)
    
    status_text = "activated" if subject.is_active else "deactivated"
    
    return schemas.SubjectToggleResponse(
        id=subject.id,
        subject_code=subject.course_code,
        subject_name=subject.name,
        is_active=subject.is_active,
        message=f"Subject {subject.course_code} has been {status_text}"
    )


@router.get(
    "/export/batch-summary",
    summary="Export Batch Summary (Excel)",
    description="Generates an Excel summary of student performance for the entire batch.",
    responses={
        200: {
            "content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {"schema": {"type": "string", "format": "binary"}}},
            "description": "Excel spreadsheet file"
        }
    }
)
@limiter.limit("5/minute")
async def export_batch_summary(
    request: Request,
    cgpa_threshold: float = Query(default=6.5, description="CGPA threshold for placement readiness"),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    require_admin(current_user)
    # The service returns a StreamingResponse directly
    return await enterprise_analytics.export_batch_summary_xlsx(db, CURRICULUM_CREDITS, cgpa_threshold=cgpa_threshold)

@router.get(
    "/export/grade-sheet/{roll_no}",
    summary="Export Student Grade Sheet (PDF)",
    description="Generates a formal PDF grade sheet for a specific student.",
    responses={
        200: {
            "content": {"application/pdf": {"schema": {"type": "string", "format": "binary"}}},
            "description": "PDF grade sheet file"
        }
    }
)
@limiter.limit("10/minute")
async def export_grade_sheet(
    request: Request,
    roll_no: str = Path(..., description="Student roll number"),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    require_admin(current_user)
    content = await enterprise_analytics.export_student_grade_sheet_pdf(db, CURRICULUM_CREDITS, roll_no=roll_no)
    return content


@router.get(
    "/export/resume/{roll_no}",
    summary="Export Student Resume (PDF)",
    description="Generates a professional resume-style PDF for a specific student.",
    responses={
        200: {
            "content": {"application/pdf": {"schema": {"type": "string", "format": "binary"}}},
            "description": "PDF resume file"
        }
    }
)
@limiter.limit("10/minute")
async def export_student_resume(
    request: Request,
    roll_no: str = Path(..., description="Student roll number"),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    require_admin(current_user)
    content = await enterprise_analytics.export_student_resume_pdf(db, CURRICULUM_CREDITS, roll_no=roll_no)
    return content

@router.get("/students", response_model=list[schemas.AdminDirectoryStudent])
async def get_admin_students(
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
    search: str = '',
    city: str = '',
    batch: str = '',
    semester: Optional[int] = Query(default=None, description="Filter by semester"),
    section: str = '',
    risk_only: bool = Query(default=False, description="Show only at-risk students"),
    sort_by: StudentSortBy = Query(default=StudentSortBy.ROLL_NO, description="Field to sort by"),
    sort_dir: SortDir = Query(default=SortDir.DESC, description="Sort direction (asc/desc)"),
    limit: int = Query(default=100, ge=1, le=100, description="Limit records (max 100)"),
):
    """
    Get a list of all students with basic filters. 
    Limited to 100 records for performance. Use /paginated for full access.
    """
    require_admin(current_user)
    credits_values = ", ".join(f"('{code}', {credit})" for code, credit in CURRICULUM_CREDITS.items())
    directory = await AdminService.build_admin_directory(db, credits_values)
    return AdminService.filter_admin_directory(directory, search, city, batch, semester, section, risk_only, sort_by.value, sort_dir, limit)

@router.get("/students/paginated", response_model=schemas.AdminDirectoryPage)
async def get_admin_students_paginated(
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
    search: str = '',
    city: str = '',
    batch: str = '',
    semester: Optional[int] = Query(default=None, description="Filter by semester"),
    section: str = '',
    risk_only: bool = Query(default=False, description="Show only at-risk students"),
    sort_by: StudentSortBy = Query(default=StudentSortBy.ROLL_NO, description="Field to sort by"),
    sort_dir: SortDir = Query(default=SortDir.DESC, description="Sort direction (asc/desc)"),
    limit: int = Query(default=20, ge=1, le=100, description="Items per page"),
    offset: int = Query(default=0, ge=0, description="Items to skip"),
):
    """
    Paginated list of students with detailed filtering and sorting.
    """
    require_admin(current_user)
    credits_values = ", ".join(f"('{code}', {credit})" for code, credit in CURRICULUM_CREDITS.items())
    directory = await AdminService.build_admin_directory(db, credits_values)
    filtered = AdminService.filter_admin_directory(directory, search, city, batch, semester, section, risk_only, sort_by.value, sort_dir, 1000)
    items = filtered[offset : offset + limit]
    return schemas.AdminDirectoryPage(
        items=items,
        pagination=schemas.PaginationMeta(total=len(filtered), limit=limit, offset=offset)
    )

@router.get("/spotlight-search", response_model=schemas.SpotlightSearchResponse)
async def get_spotlight_search(
    q: str = Query(..., min_length=2),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    require_admin(current_user)
    return await enterprise_analytics.spotlight_search(db, query=q)

@router.get(
    "/subject-leaderboard/{subject_code}", 
    response_model=schemas.SubjectLeaderboardResponse,
    summary="Get Subject Leaderboard",
    description="Retrieve top and bottom student performers for a specific subject, including rankings and percentile scores."
)
async def get_subject_leaderboard(
    subject_code: str = Path(..., description="Unique subject code"),
    section: Optional[str] = Query(default=None, description="Optional section filter (e.g., A/B/C)"),
    limit: int = Query(default=10, ge=1, le=100, description="Items per page"),
    offset: int = Query(default=0, ge=0, description="Items to skip"),
    semester: Optional[int] = Query(default=None, description="Optional semester filter"),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    require_admin(current_user)
    return await enterprise_analytics.get_subject_leaderboard(
        db, 
        CURRICULUM_CREDITS, 
        subject_code=subject_code, 
        section=section,
        limit=limit, 
        offset=offset,
        semester=semester
    )

@router.get(
    "/leaderboard/overall", 
    response_model=schemas.SubjectLeaderboardResponse,
    summary="Get Overall Institutional Leaderboard",
    description="Retrieve top and bottom student performers across all subjects based on CGPA/SGPA."
)
async def get_overall_leaderboard(
    section: Optional[str] = Query(default=None, description="Optional section filter"),
    batch: Optional[str] = Query(default=None, description="Optional batch filter"),
    limit: int = Query(default=10, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    semester: Optional[int] = Query(default=None, description="Optional semester filter"),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    require_admin(current_user)
    return await enterprise_analytics.get_overall_leaderboard(
        db, 
        CURRICULUM_CREDITS, 
        section=section,
        batch=batch,
        limit=limit, 
        offset=offset,
        semester=semester
    )

@router.get("/student-record/{roll_no}", response_model=schemas.FullStudentRecord)
async def get_student_record(
    roll_no: str = Path(..., description="Student roll number"),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    require_admin(current_user)
    student = await StudentService.get_accessible_student(
        roll_no=roll_no,
        current_user_id=current_user.id,
        role_name=current_user.role.name if current_user.role else "admin",
        db=db,
    )
    return await StudentService.build_full_student_record(student.roll_no, student_id=student.id, db=db)
@router.post("/assign-sections", response_model=schemas.MessageResponse)
async def assign_student_sections(
    batch: str = Query(..., description="Batch to process"),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    require_admin(current_user)
    count = await AdminService.assign_sections(db, batch)
    return schemas.MessageResponse(message=f"Successfully assigned sections for {count} students in batch {batch}")


@router.get(
    "/attendance/daily-report",
    response_model=schemas.DailyAttendanceReport,
    summary="Daily Attendance Report",
    description="Returns a combined subject-wise attendance report for a given date. Shows present/absent counts and percentage per subject per period, including substitute information.",
    responses={**ADMIN_RESPONSES},
)
@limiter.limit("30/minute")
async def get_daily_attendance_report(
    request: Request,
    target_date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format. Defaults to today."),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    require_admin(current_user)

    from datetime import date as dt_date
    query_date = dt_date.fromisoformat(target_date) if target_date else dt_date.today()

    # Aggregate period_attendance by subject + period with joins for names/codes
    result = await db.execute(
        select(
            models.PeriodAttendance.subject_id,
            models.Subject.name.label("subject_name"),
            models.Subject.course_code.label("course_code"),
            models.PeriodAttendance.period,
            models.Staff.name.label("marked_by_faculty_name"),
            models.PeriodAttendance.is_substitute,
            func.count(models.PeriodAttendance.id).label("total_students"),
            func.sum(
                sql_case(
                    [(models.PeriodAttendance.status.in_(['P', 'O']), 1)],
                    else_=0
                )
            ).label("present_count")
        )
        .join(models.Subject, models.PeriodAttendance.subject_id == models.Subject.id)
        .outerjoin(models.Staff, models.PeriodAttendance.marked_by_faculty_id == models.Staff.id)
        .filter(models.PeriodAttendance.date == query_date)
        .group_by(
            models.PeriodAttendance.subject_id,
            models.Subject.name,
            models.Subject.course_code,
            models.PeriodAttendance.period,
            models.Staff.name,
            models.PeriodAttendance.is_substitute
        )
        .order_by(models.PeriodAttendance.period, models.Subject.course_code)
    )
    rows = result.all()

    report_rows = []
    for row in rows:
        total = row.total_students or 0
        present = row.present_count or 0
        absent = total - present
        percentage = (present / total * 100) if total > 0 else 0.0
        
        report_rows.append(schemas.SubjectAttendanceRow(
            subject_id=row.subject_id,
            subject_name=row.subject_name,
            course_code=row.course_code,
            period=row.period,
            total_students=total,
            present_count=present,
            absent_count=absent,
            attendance_percentage=round(percentage, 2),
            marked_by_faculty_name=row.marked_by_faculty_name,
            is_substitute=row.is_substitute
        ))

    return [schemas.DailyAttendanceReport(
        date=query_date,
        rows=report_rows,
        total_periods_marked=len(report_rows),
        summary=f"Attendance overview for {query_date}"
    )]


# CGPA Ranking Endpoints

@router.get(
    "/rankings/student/{roll_no}",
    response_model=schemas.StudentRankDetails,
    summary="Get Student Rank Details",
    description="Get detailed ranking information for a specific student based on CGPA."
)
async def get_student_rank(
    roll_no: str = Path(..., description="Student roll number"),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get comprehensive ranking details for a specific student including:
    - Current rank and percentile
    - CGPA and attendance percentage
    - Performance category (Excellent, Good, Average, Needs Improvement)
    - Comparison with total student population
    """
    require_admin(current_user)
    
    from ...services.ranking_service import RankingService
    
    rank_info = await RankingService.get_student_rank_by_cgpa(
        db=db,
        roll_no=roll_no.upper(),
        curriculum_credits=CURRICULUM_CREDITS
    )
    
    if not rank_info:
        raise HTTPException(status_code=404, detail="Student not found")
    
    rank_category = RankingService.get_rank_category(
        rank=rank_info['rank'],
        total_students=rank_info['total_students']
    )
    
    return schemas.StudentRankDetails(
        roll_no=roll_no.upper(),
        name=rank_info['name'],
        rank=rank_info['rank'],
        cgpa=rank_info['cgpa'],
        attendance_percentage=rank_info['attendance_percentage'],
        percentile=rank_info['percentile'],
        total_students=rank_info['total_students'],
        backlogs=rank_info['backlogs'],
        rank_category=rank_category
    )


@router.get(
    "/rankings/batch/{batch}",
    response_model=schemas.BatchRankingsResponse,
    summary="Get Batch Rankings",
    description="Get all students in a specific batch ranked by CGPA."
)
async def get_batch_rankings(
    batch: str = Path(..., description="Batch identifier (e.g., 2021-25)"),
    limit: int = Query(default=100, ge=1, le=500, description="Number of students to return"),
    offset: int = Query(default=0, ge=0, description="Number of students to skip"),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get all students in a batch ranked by CGPA with pagination support.
    
    Returns students ordered from highest to lowest CGPA with:
    - Individual student ranking within the batch
    - CGPA and attendance percentages
    - Number of backlogs/arrears
    - Percentile ranking within batch
    """
    require_admin(current_user)
    
    from ...services.ranking_service import RankingService
    
    result = await RankingService.get_batch_rankings(
        db=db,
        batch=batch,
        curriculum_credits=CURRICULUM_CREDITS,
        limit=limit,
        offset=offset
    )
    
    return schemas.BatchRankingsResponse(
        batch=result['batch'],
        total_students=result['total_students'],
        rankings=[schemas.StudentRankingRecord(**ranking) for ranking in result['rankings']],
        has_more=result['has_more']
    )


@router.get(
    "/rankings/semester/{semester}",
    response_model=schemas.SemesterRankingsResponse,
    summary="Get Semester Rankings",
    description="Get all students in a specific semester ranked by CGPA."
)
async def get_semester_rankings(
    semester: int = Path(..., ge=1, le=8, description="Semester number"),
    limit: int = Query(default=100, ge=1, le=500, description="Number of students to return"),
    offset: int = Query(default=0, ge=0, description="Number of students to skip"),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get all students in a semester ranked by CGPA with pagination support.
    
    Returns students ordered from highest to lowest CGPA with:
    - Individual student ranking within the semester
    - CGPA and attendance percentages
    - Number of backlogs/arrears
    - Percentile ranking within semester
    """
    require_admin(current_user)
    
    from ...services.ranking_service import RankingService
    
    result = await RankingService.get_semester_rankings(
        db=db,
        semester=semester,
        curriculum_credits=CURRICULUM_CREDITS,
        limit=limit,
        offset=offset
    )
    
    return schemas.SemesterRankingsResponse(
        semester=result['semester'],
        total_students=result['total_students'],
        rankings=[schemas.SemesterRankingRecord(**ranking) for ranking in result['rankings']],
        has_more=result['has_more']
    )


@router.get(
    "/rankings/semester/{semester}/batch/{batch}",
    response_model=schemas.SemesterBatchRankingsResponse,
    summary="Get Semester-Batch Rankings",
    description="Get students in a specific semester and batch ranked by SGPA (most practical)."
)
async def get_semester_batch_rankings(
    semester: int = Path(..., ge=1, le=8, description="Semester number"),
    batch: str = Path(..., description="Batch identifier (e.g., 2021-25)"),
    limit: int = Query(default=100, ge=1, le=500, description="Number of students to return"),
    offset: int = Query(default=0, ge=0, description="Number of students to skip"),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get students in a specific semester and batch ranked by SGPA.
    
    This is often the most practical ranking as it compares students:
    - Taking the same subjects (same semester)
    - From the same academic cohort (same batch)
    - Based on semester-specific performance (SGPA not CGPA)
    
    Returns students ordered from highest to lowest SGPA with:
    - Semester-specific ranking within batch
    - SGPA for the specific semester
    - Average marks in that semester
    - Number of subjects attempted and failed
    - Percentile ranking within semester+batch combination
    """
    require_admin(current_user)
    
    from ...services.ranking_service import RankingService
    
    result = await RankingService.get_semester_batch_rankings(
        db=db,
        semester=semester,
        batch=batch,
        curriculum_credits=CURRICULUM_CREDITS,
        limit=limit,
        offset=offset
    )
    
    return schemas.SemesterBatchRankingsResponse(
        semester=result['semester'],
        batch=result['batch'],
        total_students=result['total_students'],
        rankings=[schemas.SemesterRankingRecord(**ranking) for ranking in result['rankings']],
        has_more=result['has_more']
    )


@router.get(
    "/rankings/overall",
    response_model=schemas.OverallRankingsResponse,
    summary="Get Overall Rankings",
    description="Get all students ranked by CGPA across all batches and semesters."
)
async def get_overall_rankings(
    limit: int = Query(default=100, ge=1, le=500, description="Number of students to return"),
    offset: int = Query(default=0, ge=0, description="Number of students to skip"),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get overall institution rankings based on CGPA with pagination support.
    
    Returns students ordered from highest to lowest CGPA across all batches and semesters with:
    - Institution-wide ranking
    - CGPA and attendance percentages
    - Number of backlogs/arrears
    - Percentile ranking within entire institution
    """
    require_admin(current_user)
    
    from ...services.ranking_service import RankingService
    
    result = await RankingService.get_overall_rankings(
        db=db,
        curriculum_credits=CURRICULUM_CREDITS,
        limit=limit,
        offset=offset
    )
    
    return schemas.OverallRankingsResponse(
        total_students=result['total_students'],
        rankings=[schemas.StudentRankingRecord(**ranking) for ranking in result['rankings']],
        has_more=result['has_more']
    )


@router.get(
    "/rankings/top-performers",
    response_model=List[schemas.StudentRankingRecord],
    summary="Get Top Performers",
    description="Get top N performing students based on CGPA across the institution."
)
async def get_top_performers(
    limit: int = Query(default=10, ge=1, le=50, description="Number of top performers to return"),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get the top performing students based on CGPA.
    
    Returns the highest CGPA students across the institution with:
    - Top rankings (1, 2, 3, etc.)
    - Excellent academic performance
    - High CGPA scores
    - Good attendance records
    """
    require_admin(current_user)
    
    from ...services.ranking_service import RankingService
    
    top_performers = await RankingService.get_top_performers(
        db=db,
        curriculum_credits=CURRICULUM_CREDITS,
        limit=limit
    )
    
    return [schemas.StudentRankingRecord(**student) for student in top_performers]


# Timetable Management Endpoints
@router.get(
    "/timetables",
    response_model=schemas.TimetableListResponse,
    summary="Get Timetables",
    description="Retrieve timetable entries with optional filtering by batch and section."
)
async def get_timetables(
    batch: Optional[str] = Query(None, description="Filter by batch (e.g., '2024')"),
    section: Optional[str] = Query(None, pattern=r'^[A-D]$', description="Filter by section (A, B, C, D)"),
    academic_year: Optional[str] = Query(None, description="Filter by academic year"),
    semester: Optional[int] = Query(None, ge=1, le=8, description="Filter by semester"),
    limit: int = Query(100, ge=1, le=500, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Items to skip"),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get timetable entries with filtering options."""
    require_admin(current_user)
    
    # Build query with filters
    query = select(
        models.TimeTable,
        models.Subject.name.label('subject_name'),
        models.Subject.course_code.label('subject_code'),
        models.Staff.name.label('faculty_name')
    ).outerjoin(
        models.Subject, models.TimeTable.subject_id == models.Subject.id
    ).outerjoin(
        models.Staff, models.TimeTable.faculty_id == models.Staff.id
    )
    
    # Apply filters
    if batch:
        query = query.filter(models.TimeTable.batch == batch)
    if section:
        query = query.filter(models.TimeTable.section == section)
    if academic_year:
        query = query.filter(models.TimeTable.academic_year == academic_year)
    if semester:
        query = query.filter(models.TimeTable.semester == semester)
        
    # Count total
    count_query = select(func.count(models.TimeTable.id))
    if batch:
        count_query = count_query.filter(models.TimeTable.batch == batch)
    if section:
        count_query = count_query.filter(models.TimeTable.section == section)
    if academic_year:
        count_query = count_query.filter(models.TimeTable.academic_year == academic_year)
    if semester:
        count_query = count_query.filter(models.TimeTable.semester == semester)
        
    total = (await db.execute(count_query)).scalar() or 0
    
    # Apply pagination and ordering
    query = query.order_by(
        models.TimeTable.batch,
        models.TimeTable.section, 
        models.TimeTable.day_of_week,
        models.TimeTable.period
    ).offset(offset).limit(limit)
    
    result = await db.execute(query)
    rows = result.all()
    
    items = []
    for timetable, subject_name, subject_code, faculty_name in rows:
        item_data = {
            **{c.name: getattr(timetable, c.name) for c in timetable.__table__.columns},
            'subject_name': subject_name,
            'subject_code': subject_code,
            'faculty_name': faculty_name
        }
        items.append(schemas.TimetableResponse(**item_data))
    
    return schemas.TimetableListResponse(
        items=items,
        total=total,
        batch=batch,
        section=section
    )


@router.get(
    "/timetables/weekly/{batch}/{section}",
    response_model=schemas.TimetableWeeklyView,
    summary="Get Weekly Timetable",
    description="Get a complete weekly timetable grid for a specific batch and section."
)
async def get_weekly_timetable(
    batch: str = Path(..., description="Batch (e.g., '2024')"),
    section: str = Path(..., pattern=r'^[A-D]$', description="Section (A, B, C, D)"),
    academic_year: Optional[str] = Query(None, description="Filter by academic year"),
    semester: Optional[int] = Query(None, ge=1, le=8, description="Filter by semester"),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get complete weekly timetable for a batch/section."""
    require_admin(current_user)
    
    # Get timetable entries for this batch/section
    query = select(
        models.TimeTable,
        models.Subject.name.label('subject_name'),
        models.Subject.course_code.label('subject_code'),
        models.Staff.name.label('faculty_name')
    ).outerjoin(
        models.Subject, models.TimeTable.subject_id == models.Subject.id
    ).outerjoin(
        models.Staff, models.TimeTable.faculty_id == models.Staff.id
    ).filter(
        models.TimeTable.batch == batch,
        models.TimeTable.section == section
    )
    
    if academic_year:
        query = query.filter(models.TimeTable.academic_year == academic_year)
    if semester:
        query = query.filter(models.TimeTable.semester == semester)
        
    query = query.order_by(models.TimeTable.day_of_week, models.TimeTable.period)
    
    result = await db.execute(query)
    rows = result.all()
    
    # Create weekly structure
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    days_data = {}
    
    for timetable, subject_name, subject_code, faculty_name in rows:
        day = timetable.day_of_week
        if day not in days_data:
            days_data[day] = []
        
        slot = schemas.TimetableSlot(
            period=timetable.period,
            subject_name=subject_name,
            subject_code=subject_code,
            faculty_name=faculty_name,
            room_number=timetable.room_number,
            timetable_id=timetable.id
        )
        days_data[day].append(slot)
    
    # Create days list with all 7 days (1-7)
    days = []
    for day_num in range(1, 8):
        slots = days_data.get(day_num, [])
        # Fill empty periods (1-8) with empty slots
        period_slots = {slot.period: slot for slot in slots}
        complete_slots = []
        for period in range(1, 9):
            if period in period_slots:
                complete_slots.append(period_slots[period])
            else:
                complete_slots.append(schemas.TimetableSlot(period=period))
        
        days.append(schemas.TimetableDay(
            day_of_week=day_num,
            day_name=day_names[day_num - 1],
            slots=complete_slots
        ))
    
    return schemas.TimetableWeeklyView(
        batch=batch,
        section=section,
        academic_year=academic_year,
        semester=semester,
        days=days
    )


@router.post(
    "/timetables",
    response_model=schemas.TimetableResponse,
    summary="Create Timetable Entry",
    description="Create a new timetable slot for a batch and section."
)
async def create_timetable(
    payload: schemas.TimetableCreate,
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Create a new timetable entry."""
    require_admin(current_user)
    
    # Check for conflicts (same batch, section, day, period, semester, year)
    # The database constraint is: UNIQUE (batch, section, day_of_week, period, semester, academic_year)
    # UniqueConstraint('batch', 'section', 'day_of_week', 'period', 'semester', 'academic_year', name='uq_timetable_slot')
    existing = await db.execute(
        select(models.TimeTable).filter(
            models.TimeTable.batch == payload.batch,
            models.TimeTable.section == payload.section,
            models.TimeTable.day_of_week == payload.day_of_week,
            models.TimeTable.period == payload.period,
            models.TimeTable.semester == payload.semester,
            models.TimeTable.academic_year == payload.academic_year
        )
    )
    if existing.scalars().first():
        raise HTTPException(
            status_code=400,
            detail=f"Slot already exists for {payload.batch}-{payload.section} on day {payload.day_of_week} period {payload.period}"
        )
    
    # Validate subject exists if provided
    if payload.subject_id:
        subject = await db.execute(select(models.Subject).filter(models.Subject.id == payload.subject_id))
        if not subject.scalars().first():
            raise HTTPException(status_code=400, detail="Subject not found")
    
    # Validate faculty exists if provided
    if payload.faculty_id:
        faculty = await db.execute(select(models.Staff).filter(models.Staff.id == payload.faculty_id))
        if not faculty.scalars().first():
            raise HTTPException(status_code=400, detail="Faculty not found")
    
    # Create timetable entry
    timetable = models.TimeTable(**payload.model_dump())
    db.add(timetable)
    await db.commit()
    await db.refresh(timetable)
    
    # Get related data for response
    query = select(
        models.TimeTable,
        models.Subject.name.label('subject_name'),
        models.Subject.course_code.label('subject_code'),
        models.Staff.name.label('faculty_name')
    ).outerjoin(
        models.Subject, models.TimeTable.subject_id == models.Subject.id
    ).outerjoin(
        models.Staff, models.TimeTable.faculty_id == models.Staff.id
    ).filter(models.TimeTable.id == timetable.id)
    
    result = await db.execute(query)
    row = result.first()
    timetable_obj, subject_name, subject_code, faculty_name = row
    
    response_data = {
        **{c.name: getattr(timetable_obj, c.name) for c in timetable_obj.__table__.columns},
        'subject_name': subject_name,
        'subject_code': subject_code,
        'faculty_name': faculty_name
    }
    
    return schemas.TimetableResponse(**response_data)


@router.put(
    "/timetables/{timetable_id}",
    response_model=schemas.TimetableResponse,
    summary="Update Timetable Entry",
    description="Update an existing timetable slot."
)
async def update_timetable(
    timetable_id: int = Path(..., ge=1),
    payload: schemas.TimetableUpdate = None,
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Update an existing timetable entry."""
    require_admin(current_user)
    
    if not payload:
        raise HTTPException(status_code=400, detail="No update data provided")
    
    # Get existing timetable entry
    timetable = await db.execute(select(models.TimeTable).filter(models.TimeTable.id == timetable_id))
    timetable = timetable.scalars().first()
    if not timetable:
        raise HTTPException(status_code=404, detail="Timetable entry not found")
    
    # Validate subject exists if being updated
    if payload.subject_id:
        subject = await db.execute(select(models.Subject).filter(models.Subject.id == payload.subject_id))
        if not subject.scalars().first():
            raise HTTPException(status_code=400, detail="Subject not found")
    
    # Validate faculty exists if being updated
    if payload.faculty_id:
        faculty = await db.execute(select(models.Staff).filter(models.Staff.id == payload.faculty_id))
        if not faculty.scalars().first():
            raise HTTPException(status_code=400, detail="Faculty not found")
    
    # Update fields
    update_data = payload.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(timetable, field, value)
    
    await db.commit()
    await db.refresh(timetable)
    
    # Get updated data with relations
    query = select(
        models.TimeTable,
        models.Subject.name.label('subject_name'),
        models.Subject.course_code.label('subject_code'),
        models.Staff.name.label('faculty_name')
    ).outerjoin(
        models.Subject, models.TimeTable.subject_id == models.Subject.id
    ).outerjoin(
        models.Staff, models.TimeTable.faculty_id == models.Staff.id
    ).filter(models.TimeTable.id == timetable.id)
    
    result = await db.execute(query)
    row = result.first()
    timetable_obj, subject_name, subject_code, faculty_name = row
    
    response_data = {
        **{c.name: getattr(timetable_obj, c.name) for c in timetable_obj.__table__.columns},
        'subject_name': subject_name,
        'subject_code': subject_code,
        'faculty_name': faculty_name
    }
    
    return schemas.TimetableResponse(**response_data)


@router.delete(
    "/timetables/{timetable_id}",
    status_code=204,
    summary="Delete Timetable Entry",
    description="Delete a timetable slot (soft delete by setting is_active=False)."
)
async def delete_timetable(
    timetable_id: int = Path(..., ge=1),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Delete a timetable entry."""
    require_admin(current_user)
    
    timetable = await db.execute(select(models.TimeTable).filter(models.TimeTable.id == timetable_id))
    timetable = timetable.scalars().first()
    if not timetable:
        raise HTTPException(status_code=404, detail="Timetable entry not found")
    
    # Hard delete the entry
    await db.delete(timetable)
    await db.commit()
    
    return Response(status_code=204)


# Additional endpoints for timetable management
@router.get(
    "/students/batches",
    response_model=List[str],
    summary="Get Available Batches",
    description="Get list of available student batches."
)
async def get_available_batches(
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get list of available student batches."""
    require_admin(current_user)
    
    result = await db.execute(
        select(models.Student.batch).distinct().filter(
            models.Student.batch.isnot(None)
        ).order_by(models.Student.batch.desc())
    )
    batches = [row[0] for row in result.all() if row[0]]
    return batches


@router.get(
    "/students/sections",
    response_model=List[str], 
    summary="Get Available Sections",
    description="Get list of available student sections."
)
async def get_available_sections(
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get list of available student sections."""
    require_admin(current_user)
    
    result = await db.execute(
        select(models.Student.section).distinct().filter(
            models.Student.section.isnot(None)
        ).order_by(models.Student.section)
    )
    sections = [row[0] for row in result.all() if row[0]]
    return sections


@router.get(
    "/students/semesters",
    response_model=List[int],
    summary="Get Available Semesters",
    description="Get list of available student semesters."
)
async def get_available_semesters(
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get list of available student semesters."""
    require_admin(current_user)
    
    # Get semesters from both Student.current_semester and Subject.semester
    student_semesters_result = await db.execute(
        select(models.Student.current_semester).distinct().filter(
            models.Student.current_semester.isnot(None)
        )
    )
    student_semesters = [row[0] for row in student_semesters_result.all() if row[0]]
    
    subject_semesters_result = await db.execute(
        select(models.Subject.semester).distinct().filter(
            models.Subject.semester.isnot(None)
        )
    )
    subject_semesters = [row[0] for row in subject_semesters_result.all() if row[0]]
    
    # Combine and sort unique semesters
    all_semesters = sorted(list(set(student_semesters + subject_semesters)))
    return all_semesters


@router.get(
    "/subjects",
    response_model=List[schemas.Subject],
    summary="Get Available Subjects",
    description="Get list of available subjects for timetable assignment, optionally filtered by batch, section, and semester."
)
async def get_available_subjects(
    batch: str = Query(None, description="Filter by student batch"),
    section: str = Query(None, description="Filter by student section"), 
    semester: int = Query(None, description="Filter by semester"),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get list of available subjects for timetable assignment."""
    require_admin(current_user)
    
    # Start with active subjects only
    query = select(models.Subject).filter(models.Subject.is_active == True)
    
    # If semester is provided, filter by it
    if semester is not None:
        query = query.filter(models.Subject.semester == semester)
    
    # Order by name for consistent results
    query = query.order_by(models.Subject.name)
    
    # Execute query and get results
    result = await db.execute(query)
    subjects = result.scalars().all()
    
    return subjects


@router.get(
    "/subjects/debug",
    summary="Debug Subjects",
    description="Debug endpoint to check subjects in database."
)
async def debug_subjects(
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Debug endpoint to check subjects in database."""
    require_admin(current_user)
    
    # Count all subjects
    all_subjects_result = await db.execute(select(func.count(models.Subject.id)))
    total_count = all_subjects_result.scalar()
    
    # Count active subjects
    active_subjects_result = await db.execute(
        select(func.count(models.Subject.id)).filter(models.Subject.is_active == True)
    )
    active_count = active_subjects_result.scalar()
    
    # Get sample of subjects
    sample_result = await db.execute(
        select(models.Subject.id, models.Subject.name, models.Subject.is_active, models.Subject.semester)
        .limit(5)
    )
    sample_subjects = [
        {"id": row[0], "name": row[1], "is_active": row[2], "semester": row[3]}
        for row in sample_result.all()
    ]
    
    return {
        "total_subjects": total_count,
        "active_subjects": active_count,
        "inactive_subjects": total_count - active_count,
        "sample_subjects": sample_subjects
    }
