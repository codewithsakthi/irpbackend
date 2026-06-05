from enum import Enum
from typing import Optional, List
from fastapi import APIRouter, Depends, Query, Path, HTTPException, Response, Request, UploadFile, File
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import Field

from ...core import auth
from ...core.database import get_db, settings
from ...core.constants import CURRICULUM_CREDITS, GRADE_POINTS
from ...services.student_service import StudentService
from ...models import base as models
from ...schemas import base as schemas
from ...services.admin_service import AdminService
from ...services.intelligence_engine import StudentIntelligenceEngine
from ...core.limiter import limiter
from ...professional_identity.models.models import AIProfessionalInsight
from ...services import enterprise_analytics
from sqlalchemy import select, update, delete, func, text, case as sql_case, and_
from sqlalchemy.orm import joinedload

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
    REG_NO = "reg_no"
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


async def check_threshold_schema_compatibility(db: AsyncSession):
    """Check if threshold management schema is available"""
    try:
        # Try a simple query on threshold columns
        await db.execute(text("SELECT pass_threshold FROM subjects LIMIT 1"))
        return True
    except Exception:
        raise HTTPException(
            status_code=503, 
            detail="Threshold management requires database migration. Please run 'alembic upgrade head' first."
        )


@router.put(
    "/subjects/{subject_id}/thresholds",
    response_model=schemas.SubjectThresholdResponse,
    summary="Update Subject Performance Thresholds",
    description="Update performance evaluation thresholds for a specific subject in the hybrid evaluation system."
)
async def update_subject_thresholds(
    subject_id: int = Path(description="Subject ID to update"),
    thresholds: schemas.SubjectThresholdUpdate = ...,
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    require_admin(current_user)
    
    # Check if threshold schema is available
    await check_threshold_schema_compatibility(db)
    
    # Validate subject exists
    subject_result = await db.execute(
        select(models.Subject).where(models.Subject.id == subject_id)
    )
    subject = subject_result.scalar_one_or_none()
    
    if not subject:
        raise HTTPException(status_code=404, detail="Subject not found")
    
    # Update thresholds
    await db.execute(
        update(models.Subject)
        .where(models.Subject.id == subject_id)
        .values(
            pass_threshold=thresholds.pass_threshold,
            target_average=thresholds.target_average,
            percentile_excellent=thresholds.percentile_excellent,
            percentile_good=thresholds.percentile_good,
            percentile_average=thresholds.percentile_average
        )
    )
    
    await db.commit()
    await db.refresh(subject)
    
    return schemas.SubjectThresholdResponse(
        subject_id=subject.id,
        subject_code=subject.course_code,
        subject_name=subject.name,
        updated_thresholds=thresholds
    )


@router.patch(
    "/subjects/{subject_id}/thresholds",
    response_model=schemas.SubjectThresholdResponse,
    summary="Partial Update Subject Performance Thresholds",
    description="Partially update performance evaluation thresholds for a specific subject."
)
async def patch_subject_thresholds(
    subject_id: int = Path(description="Subject ID to update"),
    threshold_patch: schemas.SubjectThresholdPatch = ...,
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    require_admin(current_user)
    
    # Check if threshold schema is available
    await check_threshold_schema_compatibility(db)
    
    # Validate subject exists
    subject_result = await db.execute(
        select(models.Subject).where(models.Subject.id == subject_id)
    )
    subject = subject_result.scalar_one_or_none()
    
    if not subject:
        raise HTTPException(status_code=404, detail="Subject not found")

    # Update thresholds with provided fields
    patch_data = threshold_patch.model_dump(exclude_unset=True)
    if patch_data:
        await db.execute(
            update(models.Subject)
            .where(models.Subject.id == subject_id)
            .values(**patch_data)
        )
        await db.commit()
        await db.refresh(subject)
    
    return schemas.SubjectThresholdResponse(
        subject_id=subject.id,
        subject_code=subject.course_code,
        subject_name=subject.name,
        updated_thresholds=schemas.SubjectThresholdUpdate(
            pass_threshold=subject.pass_threshold,
            target_average=subject.target_average,
            percentile_excellent=subject.percentile_excellent,
            percentile_good=subject.percentile_good,
            percentile_average=subject.percentile_average
        )
    )


@router.put(
    "/subjects/thresholds/batch",
    response_model=List[schemas.SubjectThresholdResponse],
    summary="Batch Update Subject Thresholds",
    description="Update performance thresholds for multiple subjects simultaneously."
)
async def batch_update_subject_thresholds(
    batch_update: schemas.SubjectThresholdBatchUpdate = ...,
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    require_admin(current_user)
    
    # Check if threshold schema is available
    await check_threshold_schema_compatibility(db)
    
    if not batch_update.subject_ids:
        raise HTTPException(status_code=400, detail="At least one subject ID is required")
    
    # Validate all subjects exist
    subjects_result = await db.execute(
        select(models.Subject).where(models.Subject.id.in_(batch_update.subject_ids))
    )
    subjects = subjects_result.scalars().all()
    
    if len(subjects) != len(batch_update.subject_ids):
        found_ids = {s.id for s in subjects}
        missing_ids = [sid for sid in batch_update.subject_ids if sid not in found_ids]
        raise HTTPException(
            status_code=404, 
            detail=f"Subjects not found: {missing_ids}"
        )
    
    # Batch update thresholds
    await db.execute(
        update(models.Subject)
        .where(models.Subject.id.in_(batch_update.subject_ids))
        .values(
            pass_threshold=batch_update.thresholds.pass_threshold,
            target_average=batch_update.thresholds.target_average,
            percentile_excellent=batch_update.thresholds.percentile_excellent,
            percentile_good=batch_update.thresholds.percentile_good,
            percentile_average=batch_update.thresholds.percentile_average
        )
    )
    
    await db.commit()
    
    # Return updated subjects
    responses = []
    for subject in subjects:
        responses.append(schemas.SubjectThresholdResponse(
            subject_id=subject.id,
            subject_code=subject.course_code,
            subject_name=subject.name,
            updated_thresholds=batch_update.thresholds
        ))
    
    return responses


@router.get(
    "/subjects/{subject_id}/thresholds",
    response_model=schemas.SubjectThresholdUpdate,
    summary="Get Subject Performance Thresholds",
    description="Retrieve current performance evaluation thresholds for a specific subject."
)
async def get_subject_thresholds(
    subject_id: int = Path(description="Subject ID to query"),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    require_admin(current_user)
    
    subject_result = await db.execute(
        select(models.Subject).where(models.Subject.id == subject_id)
    )
    subject = subject_result.scalar_one_or_none()
    
    if not subject:
        raise HTTPException(status_code=404, detail="Subject not found")
    
    return schemas.SubjectThresholdUpdate(
        pass_threshold=float(subject.pass_threshold),
        target_average=float(subject.target_average) if subject.target_average else None,
        percentile_excellent=float(subject.percentile_excellent),
        percentile_good=float(subject.percentile_good),
        percentile_average=float(subject.percentile_average)
    )


@router.post(
    "/subjects/thresholds/reset",
    response_model=schemas.MessageResponse,
    summary="Reset Subject Thresholds to Defaults",
    description="Reset performance thresholds for specified subjects to system defaults."
)
async def reset_subject_thresholds(
    request: schemas.SubjectThresholdReset,
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    require_admin(current_user)
    
    # Check if threshold schema is available
    await check_threshold_schema_compatibility(db)
    
    if not request.subject_ids:
        raise HTTPException(status_code=400, detail="At least one subject ID is required")
    
    # Reset to default values
    await db.execute(
        update(models.Subject)
        .where(models.Subject.id.in_(request.subject_ids))
        .values(
            pass_threshold=50.0,
            target_average=75.0,
            percentile_excellent=85.0,
            percentile_good=60.0,
            percentile_average=30.0
        )
    )
    
    await db.commit()
    
    return schemas.MessageResponse(
        message=f"Thresholds reset to defaults for {len(request.subject_ids)} subjects"
    )

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

@router.get(
    "/export/uploaded-resume/{roll_no}",
    summary="Get Uploaded Resume File",
    description="Serve the actual resume file uploaded by the student via SPICS.",
)
async def serve_uploaded_resume(
    roll_no: str = Path(...),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    from fastapi.responses import FileResponse
    from pathlib import Path
    require_admin(current_user)
    # Look up student id from roll_no
    row = (await db.execute(
        text("SELECT id FROM students WHERE roll_no = :r"),
        {"r": roll_no.strip().upper()},
    )).first()
    if not row:
        raise HTTPException(status_code=404, detail="Student not found")
    student_id = row[0]
    # Query SPICS profile for resume_file_path
    prof = (await db.execute(
        text("SELECT resume_file_path FROM student_professional_profiles WHERE student_id = :sid"),
        {"sid": student_id},
    )).first()
    if not prof or not prof[0]:
        raise HTTPException(status_code=404, detail="No resume uploaded by this student")
    from ...professional_identity.utils.utils import UPLOAD_BASE
    file_path = Path(prof[0])
    uploads_root = UPLOAD_BASE if UPLOAD_BASE.is_absolute() else (Path.cwd() / UPLOAD_BASE)
    uploads_root = uploads_root.resolve()
    full_path = file_path if file_path.is_absolute() else (Path.cwd() / file_path)
    full_path = full_path.resolve()
    if uploads_root not in full_path.parents and full_path != uploads_root:
        raise HTTPException(status_code=400, detail="Invalid resume file path")
    if not full_path.exists() or not full_path.is_file():
        raise HTTPException(status_code=404, detail="Resume file not found on server")
    suffix = full_path.suffix.lower()
    media_type = "application/octet-stream"
    if suffix == ".pdf":
        media_type = "application/pdf"
    elif suffix in {".doc", ".docx"}:
        media_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    return FileResponse(path=str(full_path), media_type=media_type, filename=full_path.name)

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
    items, _ = await AdminService.build_admin_directory_paginated(
        db=db,
        credits_cte_values=credits_values,
        search=search,
        city=city,
        batch=batch,
        semester=semester,
        section=section,
        risk_only=risk_only,
        sort_by=sort_by.value,
        sort_dir=sort_dir.value,
        limit=limit,
        offset=0
    )
    return items

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
    items, total = await AdminService.build_admin_directory_paginated(
        db=db,
        credits_cte_values=credits_values,
        search=search,
        city=city,
        batch=batch,
        semester=semester,
        section=section,
        risk_only=risk_only,
        sort_by=sort_by.value,
        sort_dir=sort_dir.value,
        limit=limit,
        offset=offset
    )
    return schemas.AdminDirectoryPage(
        items=items,
        pagination=schemas.PaginationMeta(total=total, limit=limit, offset=offset)
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

@router.post(
    "/students",
    response_model=schemas.AdminStudentCreateResponse,
    summary="Create a new student",
    description="Add a new student with login credentials. The initial password is set to the student's date of birth (DDMMYYYY format).",
)
async def create_student(
    payload: schemas.AdminStudentCreate,
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Create a new student account.
    - roll_no is used as the username
    - Initial password is DOB formatted as DDMMYYYY
    """
    require_admin(current_user)

    # Check roll_no uniqueness
    existing_student = await db.execute(
        select(models.Student).filter(models.Student.roll_no == payload.roll_no.upper())
    )
    if existing_student.scalars().first():
        raise HTTPException(status_code=400, detail="A student with this roll number already exists")

    username = payload.roll_no.lower()

    # Check username uniqueness
    existing_user = await db.execute(
        select(models.User).filter(models.User.username == username)
    )
    if existing_user.scalars().first():
        raise HTTPException(status_code=400, detail="Username derived from roll number already exists")

    # Check reg_no uniqueness if provided
    if payload.reg_no:
        existing_reg = await db.execute(
            select(models.Student).filter(models.Student.reg_no == payload.reg_no)
        )
        if existing_reg.scalars().first():
            raise HTTPException(status_code=400, detail="A student with this registration number already exists")

    # Resolve student role
    role_res = await db.execute(select(models.Role).filter(models.Role.name == "student"))
    student_role = role_res.scalars().first()
    if not student_role:
        raise HTTPException(status_code=500, detail="Student role not configured in database")

    # Initial password: DOB as DDMMYYYY
    initial_password = payload.dob.strftime("%d%m%Y")
    hashed_pwd = auth.get_password_hash(initial_password)

    user = models.User(
        username=username,
        password_hash=hashed_pwd,
        role_id=student_role.id,
        is_initial_password=True,
    )
    db.add(user)
    await db.flush()

    student = models.Student(
        id=user.id,
        roll_no=payload.roll_no.upper(),
        reg_no=payload.reg_no,
        name=payload.name,
        dob=payload.dob,
        email=payload.email,
        batch=payload.batch,
        section=payload.section,
        current_semester=payload.current_semester,
    )
    db.add(student)
    await db.commit()
    await db.refresh(student)

    return schemas.AdminStudentCreateResponse(
        roll_no=student.roll_no,
        name=student.name,
        username=username,
        initial_password=initial_password,
        batch=student.batch,
        current_semester=student.current_semester,
        section=student.section,
    )


@router.post(
    "/students/import",
    response_model=schemas.AdminStudentImportResponse,
    summary="Bulk import students via CSV",
    description="Import multiple students using a CSV file. Fields: roll_no, name, dob (YYYY-MM-DD, DD/MM/YYYY or DD-MM-YYYY), email, batch, reg_no, section, current_semester."
)
async def import_students(
    file: UploadFile = File(...),
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    require_admin(current_user)
    
    import csv
    import io
    from datetime import datetime
    
    # Read the file content
    contents = await file.read()
    try:
        csv_text = contents.decode("utf-8")
    except UnicodeDecodeError:
        try:
            csv_text = contents.decode("latin-1")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Could not decode file: {str(e)}")
            
    f = io.StringIO(csv_text)
    reader = csv.DictReader(f)
    
    if not reader.fieldnames:
        raise HTTPException(status_code=400, detail="CSV file is empty or has no headers")
        
    # Standardize fieldnames to lowercase and strip whitespace
    headers = [h.strip().lower() for h in reader.fieldnames if h]
    
    # Check if we have semicolon separator
    if len(headers) == 1 and ";" in reader.fieldnames[0]:
        f.seek(0)
        reader = csv.DictReader(f, delimiter=";")
        headers = [h.strip().lower() for h in (reader.fieldnames or []) if h]
        
    required_fields = {"roll_no", "name", "dob"}
    missing_fields = required_fields - set(headers)
    if missing_fields:
        raise HTTPException(
            status_code=400,
            detail=f"CSV is missing required headers: {', '.join(missing_fields)}. Found headers: {', '.join(reader.fieldnames or [])}"
        )
        
    role_res = await db.execute(select(models.Role).filter(models.Role.name == "student"))
    student_role = role_res.scalars().first()
    if not student_role:
        raise HTTPException(status_code=500, detail="Student role not configured in database")
        
    success_count = 0
    failed_count = 0
    errors = []
    
    # Helper to parse date
    def parse_date(date_str: str):
        date_str = date_str.strip()
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        raise ValueError(f"Invalid date format '{date_str}'. Supported formats: YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY")

    row_num = 1
    for row in reader:
        row_num += 1
        
        # Clean row: lower case keys, strip keys and values
        clean_row = {k.strip().lower(): (v.strip() if v else "") for k, v in row.items() if k}
        
        roll_no = clean_row.get("roll_no", "").strip().upper()
        name = clean_row.get("name", "").strip()
        dob_str = clean_row.get("dob", "").strip()
        email = clean_row.get("email", "").strip() or None
        batch = clean_row.get("batch", "").strip() or None
        reg_no = clean_row.get("reg_no", "").strip() or None
        section = clean_row.get("section", "").strip().upper() or None
        sem_str = clean_row.get("current_semester", "").strip()
        
        if not roll_no or not name or not dob_str:
            errors.append(schemas.AdminStudentImportError(
                row=row_num,
                roll_no=roll_no or None,
                error="Missing required fields: roll_no, name, or dob"
            ))
            failed_count += 1
            continue
            
        current_semester = None
        if sem_str:
            try:
                current_semester = int(sem_str)
                if not (1 <= current_semester <= 12):
                    errors.append(schemas.AdminStudentImportError(
                        row=row_num,
                        roll_no=roll_no,
                        error=f"Semester must be between 1 and 12, got {current_semester}"
                    ))
                    failed_count += 1
                    continue
            except ValueError:
                errors.append(schemas.AdminStudentImportError(
                    row=row_num,
                    roll_no=roll_no,
                    error=f"Invalid semester format '{sem_str}', must be a number"
                ))
                failed_count += 1
                continue
                
        try:
            dob_val = parse_date(dob_str)
        except ValueError as e:
            errors.append(schemas.AdminStudentImportError(
                row=row_num,
                roll_no=roll_no,
                error=str(e)
            ))
            failed_count += 1
            continue
            
        # Check roll_no uniqueness
        existing_student = await db.execute(
            select(models.Student).filter(models.Student.roll_no == roll_no)
        )
        if existing_student.scalars().first():
            errors.append(schemas.AdminStudentImportError(
                row=row_num,
                roll_no=roll_no,
                error=f"Student with roll number '{roll_no}' already exists"
            ))
            failed_count += 1
            continue
            
        username = roll_no.lower()
        existing_user = await db.execute(
            select(models.User).filter(models.User.username == username)
        )
        if existing_user.scalars().first():
            errors.append(schemas.AdminStudentImportError(
                row=row_num,
                roll_no=roll_no,
                error=f"Username '{username}' already exists"
            ))
            failed_count += 1
            continue
            
        if reg_no:
            existing_reg = await db.execute(
                select(models.Student).filter(models.Student.reg_no == reg_no)
            )
            if existing_reg.scalars().first():
                errors.append(schemas.AdminStudentImportError(
                    row=row_num,
                    roll_no=roll_no,
                    error=f"Student with registration number '{reg_no}' already exists"
                ))
                failed_count += 1
                continue
                
        # Create student user
        try:
            initial_password = dob_val.strftime("%d%m%Y")
            hashed_pwd = auth.get_password_hash(initial_password)
            
            user = models.User(
                username=username,
                password_hash=hashed_pwd,
                role_id=student_role.id,
                is_initial_password=True,
            )
            db.add(user)
            await db.flush()
            
            student = models.Student(
                id=user.id,
                roll_no=roll_no,
                reg_no=reg_no,
                name=name,
                dob=dob_val,
                email=email,
                batch=batch,
                section=section,
                current_semester=current_semester,
            )
            db.add(student)
            await db.flush()
            success_count += 1
        except Exception as e:
            errors.append(schemas.AdminStudentImportError(
                row=row_num,
                roll_no=roll_no,
                error=f"Database insertion error: {str(e)}"
            ))
            failed_count += 1
            continue
            
    await db.commit()
    
    return schemas.AdminStudentImportResponse(
        total_records=row_num - 1,
        success_count=success_count,
        failed_count=failed_count,
        errors=errors
    )


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

# --- ASIE ADMIN ENDPOINTS ---

@router.get(
    "/talent-matrix",
    response_model=schemas.AdminTalentMatrixResponse,
    summary="Get Cohort Talent Matrix",
    description="Retrieve the 9-box performance vs potential cohort talent matrix."
)
async def get_talent_matrix(
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    require_admin(current_user)
    
    # Fetch all non-deleted students (no expensive joinedload needed for this endpoint)
    students_res = await db.execute(
        select(models.Student).filter(models.Student.is_deleted == False)
    )
    students = students_res.scalars().all()
    
    student_ids = [s.id for s in students]
    if not student_ids:
        return schemas.AdminTalentMatrixResponse(items=[], quadrant_counts={
            "Star Performer": 0, "Academic Pillar": 0, "Curriculum Specialist": 0,
            "High Potential Leader": 0, "Balanced Core": 0, "Solid Contributor": 0,
            "Hidden Talent": 0, "Growth Candidate": 0, "Needs Developmental Support": 0
        })

    # ── Batch-fetch 1: All capability scores in one query ──────────────────────
    cap_scores_res = await db.execute(
        select(models.StudentCapabilityScore).filter(
            models.StudentCapabilityScore.student_id.in_(student_ids)
        )
    )
    cap_scores_map: dict = {cs.student_id: cs for cs in cap_scores_res.scalars().all()}

    # ── Batch-fetch 2: CGPA via single SQL aggregation (replaces 90x calculate_analytics) ──
    cgpa_rows = (await db.execute(text("""
        WITH grade_points AS (
            SELECT
                sa.student_id,
                CASE sa.grade
                    WHEN 'O'  THEN 10.0
                    WHEN 'A+' THEN 9.0
                    WHEN 'A'  THEN 8.0
                    WHEN 'B+' THEN 7.0
                    WHEN 'B'  THEN 6.0
                    WHEN 'C'  THEN 5.0
                    WHEN 'P'  THEN 4.0
                    ELSE 0.0
                END AS grade_point,
                COALESCE(sub.credits, 3.0) AS credits
            FROM student_assessments sa
            JOIN subjects sub ON sub.id = sa.subject_id
            WHERE sa.student_id = ANY(:sids)
              AND sa.assessment_type = 'SEMESTER_EXAM'
              AND sa.is_final = true
              AND sa.grade IS NOT NULL
              AND sub.is_active = true
        )
        SELECT
            student_id,
            CASE WHEN SUM(credits) > 0
                THEN ROUND(SUM(grade_point * credits) / SUM(credits), 2)
                ELSE 0.0
            END AS cgpa
        FROM grade_points
        GROUP BY student_id
    """), {"sids": student_ids})).mappings().all()
    cgpa_map: dict = {row["student_id"]: float(row["cgpa"]) for row in cgpa_rows}

    # ── Batch-fetch 3: SPICS professional identity data ────────────────────────
    spics_map: dict = {}
    spics_rows = (await db.execute(text("""
        SELECT 
            spp.student_id,
            ROUND(spp.profile_completion_score::numeric, 2) AS profile_completion_score,
            spp.github_username,
            (SELECT COUNT(*) FROM student_projects sp WHERE sp.student_id = spp.student_id) AS projects_count,
            (SELECT COUNT(*) FROM student_skills sk WHERE sk.student_id = spp.student_id) AS skills_count,
            (SELECT COUNT(*) FROM student_certifications sc WHERE sc.student_id = spp.student_id) AS certs_count,
            (SELECT ai.career_readiness_score FROM ai_professional_insights ai WHERE ai.student_id = spp.student_id) AS career_readiness_score
        FROM student_professional_profiles spp
        WHERE spp.student_id = ANY(:sids)
    """), {"sids": student_ids})).mappings().all()
    for row in spics_rows:
        spics_map[row["student_id"]] = row

    # ── Compute scores for any students missing cached capability scores ────────
    missing_ids = [sid for sid in student_ids if sid not in cap_scores_map]
    if missing_ids:
        for sid in missing_ids:
            try:
                await StudentIntelligenceEngine.analyze_and_cache_student(sid, db, bypass_ai=True)
                refreshed = await db.get(models.StudentCapabilityScore, sid)
                if refreshed:
                    cap_scores_map[sid] = refreshed
            except Exception:
                pass

    # ── Build response ─────────────────────────────────────────────────────────
    items = []
    quadrant_counts = {
        "Star Performer": 0, "Academic Pillar": 0, "Curriculum Specialist": 0,
        "High Potential Leader": 0, "Balanced Core": 0, "Solid Contributor": 0,
        "Hidden Talent": 0, "Growth Candidate": 0, "Needs Developmental Support": 0
    }
    
    for student in students:
        cap_score = cap_scores_map.get(student.id)
        if not cap_score:
            continue
            
        academic = float(cap_score.academic_score or 0)
        spi = float(cap_score.spi_score or 0)
        profile_type = cap_score.profile_type or "Balanced Performer"
        cgpa = cgpa_map.get(student.id, 0.0)
        
        # Classify into 9-box matrix quadrants
        if academic >= 80:
            x_band = "High"
        elif academic >= 60:
            x_band = "Medium"
        else:
            x_band = "Low"
            
        if spi >= 80:
            y_band = "High"
        elif spi >= 60:
            y_band = "Medium"
        else:
            y_band = "Low"
            
        if x_band == "High" and y_band == "High":
            quadrant = "Star Performer"
        elif x_band == "High" and y_band == "Medium":
            quadrant = "Academic Pillar"
        elif x_band == "High" and y_band == "Low":
            quadrant = "Curriculum Specialist"
        elif x_band == "Medium" and y_band == "High":
            quadrant = "High Potential Leader"
        elif x_band == "Medium" and y_band == "Medium":
            quadrant = "Balanced Core"
        elif x_band == "Medium" and y_band == "Low":
            quadrant = "Solid Contributor"
        elif x_band == "Low" and y_band == "High":
            quadrant = "Hidden Talent"
        elif x_band == "Low" and y_band == "Medium":
            quadrant = "Growth Candidate"
        else:
            quadrant = "Needs Developmental Support"
            
        quadrant_counts[quadrant] += 1
        
        sp_data = spics_map.get(student.id, {})
        items.append(schemas.AdminTalentMatrixItem(
            roll_no=student.roll_no,
            name=student.name,
            academic_score=academic,
            spi_score=spi,
            profile_type=profile_type,
            quadrant=quadrant,
            cgpa=cgpa,
            profile_completion_score=sp_data.get("profile_completion_score"),
            projects_count=int(sp_data.get("projects_count") or 0),
            skills_count=int(sp_data.get("skills_count") or 0),
            certifications_count=int(sp_data.get("certs_count") or 0),
            career_readiness_score=sp_data.get("career_readiness_score"),
            github_connected=bool(sp_data.get("github_username")),
        ))
        
    return schemas.AdminTalentMatrixResponse(items=items, quadrant_counts=quadrant_counts)

@router.get(
    "/hidden-talents",
    response_model=schemas.AdminHiddenTalentsResponse,
    summary="Get Hidden Talents",
    description="Retrieve students with lower CGPA but outstanding technical or extracurricular scores."
)
async def get_hidden_talents(
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    require_admin(current_user)

    students_res = await db.execute(
        select(models.Student).filter(models.Student.is_deleted == False)
    )
    students = students_res.scalars().all()
    student_ids = [s.id for s in students]
    if not student_ids:
        return schemas.AdminHiddenTalentsResponse(items=[])

    # ── Batch-fetch capability scores ─────────────────────────────────────────
    cap_rows = (await db.execute(
        select(models.StudentCapabilityScore).filter(
            models.StudentCapabilityScore.student_id.in_(student_ids)
        )
    )).scalars().all()
    cap_map: dict = {cs.student_id: cs for cs in cap_rows}

    # ── Batch-fetch CGPA for all students ─────────────────────────────────────
    cgpa_rows = (await db.execute(text("""
        WITH gp AS (
            SELECT sa.student_id,
                   CASE sa.grade
                       WHEN 'O'  THEN 10.0 WHEN 'A+' THEN 9.0 WHEN 'A'  THEN 8.0
                       WHEN 'B+' THEN 7.0  WHEN 'B'  THEN 6.0 WHEN 'C'  THEN 5.0
                       WHEN 'P'  THEN 4.0  ELSE 0.0
                   END AS grade_point,
                   COALESCE(sub.credits, 3.0) AS credits
            FROM student_assessments sa
            JOIN subjects sub ON sub.id = sa.subject_id
            WHERE sa.student_id = ANY(:sids)
              AND sa.assessment_type = 'SEMESTER_EXAM'
              AND sa.is_final = true AND sa.grade IS NOT NULL AND sub.is_active = true
        )
        SELECT student_id,
               CASE WHEN SUM(credits) > 0
                    THEN ROUND(SUM(grade_point * credits) / SUM(credits), 2)
                    ELSE 0.0 END AS cgpa
        FROM gp GROUP BY student_id
    """), {"sids": student_ids})).mappings().all()
    cgpa_map: dict = {r["student_id"]: float(r["cgpa"]) for r in cgpa_rows}

    # ── Batch-fetch extra-curricular counts ───────────────────────────────────
    ec_rows = (await db.execute(text("""
        SELECT student_id, COUNT(*) AS ec_count
        FROM extra_curricular
        WHERE student_id = ANY(:sids)
        GROUP BY student_id
    """), {"sids": student_ids})).mappings().all()
    ec_map: dict = {r["student_id"]: int(r["ec_count"]) for r in ec_rows}

    items = []
    for student in students:
        cap_score = cap_map.get(student.id)
        if not cap_score:
            continue
        cap_dto = schemas.StudentCapabilityScoreResponse.model_validate(cap_score)
        cgpa = cgpa_map.get(student.id, 0.0)

        if cgpa < 7.0:
            outstanding = []
            if cap_dto.technical_score >= 75:
                outstanding.append(f"Technical ({cap_dto.technical_score:.0f})")
            if cap_dto.leadership_score >= 75:
                outstanding.append(f"Leadership ({cap_dto.leadership_score:.0f})")
            if cap_dto.sports_score >= 75:
                outstanding.append(f"Sports ({cap_dto.sports_score:.0f})")
            if cap_dto.creativity_score >= 75:
                outstanding.append(f"Creativity ({cap_dto.creativity_score:.0f})")

            if outstanding:
                reason = f"Lower academic proxy ({cgpa:.2f}) but shows exceptional strengths in: {', '.join(outstanding)}."
                items.append(schemas.AdminHiddenTalentItem(
                    roll_no=student.roll_no,
                    name=student.name,
                    cgpa=cgpa,
                    technical_score=float(cap_dto.technical_score),
                    leadership_score=float(cap_dto.leadership_score),
                    sports_score=float(cap_dto.sports_score),
                    creativity_score=float(cap_dto.creativity_score),
                    extra_curricular_count=ec_map.get(student.id, 0),
                    highlight_reason=reason
                ))

    return schemas.AdminHiddenTalentsResponse(items=items)

@router.get(
    "/high-potential",
    response_model=schemas.AdminHighPotentialResponse,
    summary="Get High Potential Students",
    description="Retrieve students with high SPI (Student Potential Index)."
)
async def get_high_potential(
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    require_admin(current_user)
    
    matrix = await get_talent_matrix(current_user, db)
    # Filter SPI score >= 70, order by SPI descending
    high_potentials = [item for item in matrix.items if item.spi_score >= 70]
    high_potentials.sort(key=lambda x: x.spi_score, reverse=True)
    
    return schemas.AdminHighPotentialResponse(items=high_potentials)

@router.get(
    "/intervention-engine",
    response_model=schemas.AdminInterventionResponse,
    summary="ASIE Intervention Engine",
    description="Identify students needing developmental support with risk analysis and suggested actions."
)
async def get_intervention_engine(
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    require_admin(current_user)

    students_res = await db.execute(
        select(models.Student).filter(models.Student.is_deleted == False)
    )
    students = students_res.scalars().all()
    student_ids = [s.id for s in students]
    if not student_ids:
        return schemas.AdminInterventionResponse(items=[])

    # ── Batch-fetch capability scores ─────────────────────────────────────────
    cap_rows = (await db.execute(
        select(models.StudentCapabilityScore).filter(
            models.StudentCapabilityScore.student_id.in_(student_ids)
        )
    )).scalars().all()
    cap_map: dict = {cs.student_id: cs for cs in cap_rows}

    # ── Batch-fetch CGPA ──────────────────────────────────────────────────────
    cgpa_rows = (await db.execute(text("""
        WITH gp AS (
            SELECT sa.student_id,
                   CASE sa.grade
                       WHEN 'O'  THEN 10.0 WHEN 'A+' THEN 9.0 WHEN 'A'  THEN 8.0
                       WHEN 'B+' THEN 7.0  WHEN 'B'  THEN 6.0 WHEN 'C'  THEN 5.0
                       WHEN 'P'  THEN 4.0  ELSE 0.0
                   END AS grade_point,
                   COALESCE(sub.credits, 3.0) AS credits
            FROM student_assessments sa
            JOIN subjects sub ON sub.id = sa.subject_id
            WHERE sa.student_id = ANY(:sids)
              AND sa.assessment_type = 'SEMESTER_EXAM'
              AND sa.is_final = true AND sa.grade IS NOT NULL AND sub.is_active = true
        )
        SELECT student_id,
               CASE WHEN SUM(credits) > 0
                    THEN ROUND(SUM(grade_point * credits) / SUM(credits), 2)
                    ELSE 0.0 END AS cgpa
        FROM gp GROUP BY student_id
    """), {"sids": student_ids})).mappings().all()
    cgpa_map: dict = {r["student_id"]: float(r["cgpa"]) for r in cgpa_rows}

    # ── Batch-fetch attendance percentage for all students ────────────────────
    att_rows = (await db.execute(text("""
        SELECT student_id,
               CASE WHEN SUM(total_periods) > 0
                    THEN ROUND(
                        (SUM(present) + SUM(on_duty))::numeric
                        / SUM(total_periods) * 100, 2
                    )
                    ELSE 0.0 END AS attendance_pct
        FROM v_attendance_summary
        WHERE student_id = ANY(:sids)
        GROUP BY student_id
    """), {"sids": student_ids})).mappings().all()
    att_map: dict = {r["student_id"]: float(r["attendance_pct"]) for r in att_rows}

    items = []
    for student in students:
        cap_score = cap_map.get(student.id)
        if not cap_score:
            continue
        cap_dto = schemas.StudentCapabilityScoreResponse.model_validate(cap_score)
        profile_type = cap_score.profile_type or "Balanced Performer"
        cgpa = cgpa_map.get(student.id, 0.0)
        attendance = att_map.get(student.id, 0.0)

        # Criteria: profile is Needs Support or Academic/Discipline score < 50
        if profile_type == "Needs Support" or cap_dto.academic_score < 50 or cap_dto.discipline_score < 50:
            if cap_dto.academic_score < 40 or attendance < 65:
                risk_level = "Critical"
                action = "Initiate formal counseling diary meeting and draft an academic recovery plan."
            elif cap_dto.academic_score < 50 or attendance < 75:
                risk_level = "High"
                action = "Schedule peer tutoring sessions and alert class counselor."
            else:
                risk_level = "Moderate"
                action = "Monitor closely and request regular attendance reviews."

            items.append(schemas.AdminInterventionItem(
                roll_no=student.roll_no,
                name=student.name,
                cgpa=cgpa,
                attendance_percentage=attendance,
                academic_score=float(cap_dto.academic_score),
                discipline_score=float(cap_dto.discipline_score),
                profile_type=profile_type,
                suggested_action=action,
                risk_level=risk_level
            ))

    return schemas.AdminInterventionResponse(items=items)

@router.get(
    "/career-distribution",
    response_model=schemas.AdminCareerDistributionResponse,
    summary="Get Career Fit Distribution",
    description="Retrieve cohort aggregate of predicted career fit distributions."
)
async def get_career_distribution(
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    require_admin(current_user)
    
    profiles_res = await db.execute(select(AIProfessionalInsight))
    profiles = profiles_res.scalars().all()
    
    domain_counts = {}
    total_fit_records = 0
    
    # Pre-calculated distribution from AI career fit mappings
    for profile in profiles:
        career_fit = profile.career_fit_roles or []
        if career_fit:
            # Find domain with highest match percentage for this student
            sorted_fits = sorted(career_fit, key=lambda x: x.get("match_percentage", 0.0), reverse=True)
            if sorted_fits:
                top_domain = sorted_fits[0].get("domain", "General Operations")
                domain_counts[top_domain] = domain_counts.get(top_domain, 0) + 1
                total_fit_records += 1
                
    # Fallback to heuristics if AI profiles aren't populated yet — batch query
    if total_fit_records == 0:
        cap_rows = (await db.execute(
            select(models.StudentCapabilityScore)
        )).scalars().all()
        for cap_score in cap_rows:
            tech = float(cap_score.technical_score or 0)
            lead = float(cap_score.leadership_score or 0)
            academic = float(cap_score.academic_score or 0)
            if tech >= 75:
                top_domain = "Software Engineering"
            elif lead >= 70:
                top_domain = "Product Management"
            elif academic >= 75:
                top_domain = "Research"
            else:
                top_domain = "Operations"
            domain_counts[top_domain] = domain_counts.get(top_domain, 0) + 1
            total_fit_records += 1

    distribution = []
    for domain, count in domain_counts.items():
        percentage = (count / total_fit_records * 100.0) if total_fit_records > 0 else 0.0
        distribution.append(schemas.CareerDistributionItem(
            domain=domain,
            count=count,
            percentage=round(percentage, 1)
        ))

    distribution.sort(key=lambda x: x.count, reverse=True)
    return schemas.AdminCareerDistributionResponse(distribution=distribution)
