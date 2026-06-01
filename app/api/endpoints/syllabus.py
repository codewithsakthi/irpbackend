"""
Syllabus Tracking Endpoints
----------------------------
Staff  : manage their own syllabus plans & update progress
HOD/Admin : view all staff syllabus coverage overview
"""
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Path, Body
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete

from ...core import auth
from ...core.database import get_db
from ...models import base as models
from ...schemas import base as schemas

router = APIRouter(tags=["Syllabus"])

ALLOWED_STAFF_ROLES = {"staff", "faculty", "hod", "director", "admin"}
HOD_ROLES = {"hod", "director", "admin"}


def _require_staff(user: models.User):
    role = user.role.name.lower() if user.role else ""
    if role not in ALLOWED_STAFF_ROLES:
        raise HTTPException(status_code=403, detail="Staff access required")


def _require_hod(user: models.User):
    role = user.role.name.lower() if user.role else ""
    if role not in HOD_ROLES:
        raise HTTPException(status_code=403, detail="HOD / Admin access required")


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _build_plan_response(plan: models.SyllabusPlan) -> schemas.SyllabusPlanResponse:
    progress = plan.progress
    covered = progress.covered_periods if progress else 0
    total = plan.total_periods or 0
    remaining = max(0, total - covered)
    pct = round((covered / total * 100), 1) if total > 0 else 0.0
    return schemas.SyllabusPlanResponse(
        id=plan.id,
        subject_id=plan.subject_id,
        subject_name=plan.subject.name if plan.subject else "",
        course_code=plan.subject.course_code if plan.subject else "",
        faculty_id=plan.faculty_id,
        faculty_name=plan.faculty.name if plan.faculty else "",
        academic_year=plan.academic_year,
        section=plan.section,
        unit_number=plan.unit_number,
        unit_title=plan.unit_title,
        total_periods=total,
        covered_periods=covered,
        remaining_periods=remaining,
        completion_percentage=pct,
        notes=progress.notes if progress else None,
        last_updated=progress.last_updated if progress else None,
    )


def _subject_summary(subject_id: int, plans: list[models.SyllabusPlan]) -> schemas.SyllabusSubjectSummary:
    subj_plans = [p for p in plans if p.subject_id == subject_id]
    if not subj_plans:
        return None
    ref = subj_plans[0]
    unit_responses = sorted([_build_plan_response(p) for p in subj_plans], key=lambda r: r.unit_number)
    total_planned = sum(p.total_periods for p in subj_plans)
    total_covered = sum((p.progress.covered_periods if p.progress else 0) for p in subj_plans)
    completed = sum(1 for r in unit_responses if r.completion_percentage >= 100.0)
    pct = round((total_covered / total_planned * 100), 1) if total_planned > 0 else 0.0
    return schemas.SyllabusSubjectSummary(
        subject_id=subject_id,
        subject_name=ref.subject.name if ref.subject else "",
        course_code=ref.subject.course_code if ref.subject else "",
        semester=ref.subject.semester if ref.subject else None,
        section=ref.section,
        total_units=len(subj_plans),
        completed_units=completed,
        total_planned_periods=total_planned,
        total_covered_periods=total_covered,
        overall_completion_percentage=pct,
        units=unit_responses,
    )


# ─── Staff: view own syllabus progress ───────────────────────────────────────

@router.get(
    "/my-progress",
    response_model=List[schemas.SyllabusSubjectSummary],
    summary="Get My Syllabus Progress",
    description="Returns the logged-in staff member's syllabus plans grouped by subject.",
)
async def get_my_syllabus_progress(
    academic_year: Optional[str] = Query(None, description="Filter by academic year, e.g. 2024-25"),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    _require_staff(current_user)

    # 1. Fetch assigned subjects for this staff member
    assignment_stmt = (
        select(models.FacultySubjectAssignment)
        .where(models.FacultySubjectAssignment.faculty_id == current_user.id)
    )
    if academic_year:
        assignment_stmt = assignment_stmt.where(
            (models.FacultySubjectAssignment.academic_year == academic_year) |
            (models.FacultySubjectAssignment.academic_year.is_(None))
        )
    assignment_result = await db.execute(assignment_stmt)
    assignments = assignment_result.scalars().all()

    for a in assignments:
        await db.refresh(a, ["subject"])

    # 2. Fetch syllabus plans
    stmt = (
        select(models.SyllabusPlan)
        .where(models.SyllabusPlan.faculty_id == current_user.id)
        .order_by(models.SyllabusPlan.subject_id, models.SyllabusPlan.unit_number)
    )
    if academic_year:
        stmt = stmt.where(models.SyllabusPlan.academic_year == academic_year)

    result = await db.execute(stmt)
    plans: list[models.SyllabusPlan] = result.scalars().all()

    for plan in plans:
        await db.refresh(plan, ["subject", "faculty", "progress"])

    plans_by_subject = {}
    for plan in plans:
        plans_by_subject.setdefault(plan.subject_id, []).append(plan)

    summaries = []
    processed_subject_ids = set()

    for a in assignments:
        subj = a.subject
        if not subj or not subj.is_active:
            continue
        processed_subject_ids.add(subj.id)
        
        subj_plans = plans_by_subject.get(subj.id, [])
        if subj_plans:
            summary = _subject_summary(subj.id, plans)
            if summary:
                if not summary.section:
                    summary.section = a.section
                summaries.append(summary)
        else:
            summaries.append(schemas.SyllabusSubjectSummary(
                subject_id=subj.id,
                subject_name=subj.name,
                course_code=subj.course_code,
                semester=subj.semester,
                section=a.section,
                total_units=0,
                completed_units=0,
                total_planned_periods=0,
                total_covered_periods=0,
                overall_completion_percentage=0.0,
                units=[],
            ))

    # Add plans that are not in explicit assignments
    for sid, subj_plans in plans_by_subject.items():
        if sid not in processed_subject_ids:
            summary = _subject_summary(sid, plans)
            if summary:
                summaries.append(summary)

    return summaries


# ─── Staff: create a syllabus plan (unit) ────────────────────────────────────

@router.post(
    "/plan",
    response_model=schemas.SyllabusPlanResponse,
    summary="Create Syllabus Plan Unit",
    description="Staff creates a unit entry for their subject syllabus. HOD/admin can also create for any staff.",
)
async def create_syllabus_plan(
    payload: schemas.SyllabusPlanCreate = Body(...),
    staff_id: Optional[int] = Query(None, description="HOD/admin: target staff ID. Omit for self."),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    _require_staff(current_user)
    role = current_user.role.name.lower() if current_user.role else ""

    # Determine effective faculty_id
    effective_faculty_id = current_user.id
    if staff_id and role in HOD_ROLES:
        effective_faculty_id = staff_id
    elif staff_id and role not in HOD_ROLES:
        raise HTTPException(status_code=403, detail="Only HOD/admin can create plans for other staff")

    # Verify assignment if regular staff (must be assigned to the subject)
    if role not in HOD_ROLES:
        assignment = await db.execute(
            select(models.FacultySubjectAssignment)
            .where(
                models.FacultySubjectAssignment.faculty_id == effective_faculty_id,
                models.FacultySubjectAssignment.subject_id == payload.subject_id,
            )
        )
        if not assignment.scalars().first():
            raise HTTPException(status_code=403, detail="You are not assigned to this subject")

    # Verify subject exists
    subject_res = await db.execute(select(models.Subject).where(models.Subject.id == payload.subject_id))
    subject = subject_res.scalars().first()
    if not subject:
        raise HTTPException(status_code=404, detail="Subject not found")

    # Check for duplicate unit
    existing = await db.execute(
        select(models.SyllabusPlan).where(
            models.SyllabusPlan.subject_id == payload.subject_id,
            models.SyllabusPlan.faculty_id == effective_faculty_id,
            models.SyllabusPlan.academic_year == payload.academic_year,
            models.SyllabusPlan.unit_number == payload.unit_number,
        )
    )
    if payload.section:
        existing = await db.execute(
            select(models.SyllabusPlan).where(
                models.SyllabusPlan.subject_id == payload.subject_id,
                models.SyllabusPlan.faculty_id == effective_faculty_id,
                models.SyllabusPlan.academic_year == payload.academic_year,
                models.SyllabusPlan.section == payload.section,
                models.SyllabusPlan.unit_number == payload.unit_number,
            )
        )
    if existing.scalars().first():
        raise HTTPException(status_code=409, detail=f"Unit {payload.unit_number} already exists for this subject/faculty/year")

    plan = models.SyllabusPlan(
        subject_id=payload.subject_id,
        faculty_id=effective_faculty_id,
        academic_year=payload.academic_year,
        section=payload.section,
        unit_number=payload.unit_number,
        unit_title=payload.unit_title,
        total_periods=payload.total_periods,
    )
    db.add(plan)
    await db.flush()

    # Create empty progress entry
    progress = models.SyllabusProgress(
        plan_id=plan.id,
        faculty_id=effective_faculty_id,
        covered_periods=0,
        updated_by=current_user.id,
    )
    db.add(progress)
    await db.commit()
    await db.refresh(plan)
    await db.refresh(plan, ["subject", "faculty", "progress"])

    return _build_plan_response(plan)


# ─── Staff: update progress for a unit ───────────────────────────────────────

@router.patch(
    "/progress/{plan_id}",
    response_model=schemas.SyllabusPlanResponse,
    summary="Update Syllabus Progress",
    description="Staff updates how many periods they have covered for a specific unit.",
)
async def update_syllabus_progress(
    plan_id: int = Path(...),
    payload: schemas.SyllabusProgressUpdate = Body(...),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    _require_staff(current_user)
    role = current_user.role.name.lower() if current_user.role else ""

    plan_res = await db.execute(select(models.SyllabusPlan).where(models.SyllabusPlan.id == plan_id))
    plan = plan_res.scalars().first()
    if not plan:
        raise HTTPException(status_code=404, detail="Syllabus plan unit not found")

    # Only the assigned faculty or HOD/admin can update
    if plan.faculty_id != current_user.id and role not in HOD_ROLES:
        raise HTTPException(status_code=403, detail="You can only update your own syllabus progress")

    # Validate covered <= total
    if payload.covered_periods > plan.total_periods:
        raise HTTPException(
            status_code=422,
            detail=f"covered_periods ({payload.covered_periods}) cannot exceed total_periods ({plan.total_periods})"
        )

    await db.refresh(plan, ["progress"])
    if plan.progress:
        await db.execute(
            update(models.SyllabusProgress)
            .where(models.SyllabusProgress.plan_id == plan_id)
            .values(
                covered_periods=payload.covered_periods,
                notes=payload.notes,
                last_updated=datetime.utcnow(),
                updated_by=current_user.id,
            )
        )
    else:
        progress = models.SyllabusProgress(
            plan_id=plan.id,
            faculty_id=plan.faculty_id,
            covered_periods=payload.covered_periods,
            notes=payload.notes,
            updated_by=current_user.id,
        )
        db.add(progress)

    await db.commit()
    await db.refresh(plan)
    await db.refresh(plan, ["subject", "faculty", "progress"])
    return _build_plan_response(plan)


# ─── HOD/Admin: full department overview ─────────────────────────────────────

@router.get(
    "/hod-overview",
    response_model=schemas.SyllabusHODOverviewResponse,
    summary="HOD: Syllabus Overview for All Staff",
    description="Returns all staff members with their syllabus plans and coverage summary. HOD/Admin only.",
)
async def get_hod_syllabus_overview(
    academic_year: Optional[str] = Query(None, description="Filter by academic year"),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    _require_staff(current_user)
    role = current_user.role.name.lower() if current_user.role else ""
    if role not in HOD_ROLES:
        raise HTTPException(status_code=403, detail="HOD or Admin access required for department overview")

    # 1. Fetch assignments for grouping
    assignment_stmt = select(models.FacultySubjectAssignment)
    if academic_year:
        assignment_stmt = assignment_stmt.where(
            (models.FacultySubjectAssignment.academic_year == academic_year) |
            (models.FacultySubjectAssignment.academic_year.is_(None))
        )
    assignment_result = await db.execute(assignment_stmt)
    all_assignments = assignment_result.scalars().all()
    for a in all_assignments:
        await db.refresh(a, ["subject"])

    assignment_map: dict[int, list] = {}
    for a in all_assignments:
        assignment_map.setdefault(a.faculty_id, []).append(a)

    # 2. Get syllabus plans (optionally filtered by year)
    stmt = (
        select(models.SyllabusPlan)
        .order_by(models.SyllabusPlan.faculty_id, models.SyllabusPlan.subject_id, models.SyllabusPlan.unit_number)
    )
    if academic_year:
        stmt = stmt.where(models.SyllabusPlan.academic_year == academic_year)

    result = await db.execute(stmt)
    all_plans: list[models.SyllabusPlan] = result.scalars().all()

    for plan in all_plans:
        await db.refresh(plan, ["subject", "faculty", "progress"])

    faculty_map: dict[int, list] = {}
    for plan in all_plans:
        faculty_map.setdefault(plan.faculty_id, []).append(plan)

    plans_by_faculty_subject = {}
    for plan in all_plans:
        plans_by_faculty_subject.setdefault((plan.faculty_id, plan.subject_id), []).append(plan)

    all_staff_res = await db.execute(
        select(models.Staff).order_by(models.Staff.name)
    )
    all_staff: list[models.Staff] = all_staff_res.scalars().all()

    used_year = academic_year or "All Years"
    staff_overviews: list[schemas.SyllabusStaffOverview] = []
    dept_total_planned = 0
    dept_total_covered = 0

    for staff_member in all_staff:
        plans = faculty_map.get(staff_member.id, [])
        assignments = assignment_map.get(staff_member.id, [])
        
        subject_summaries = []
        staff_total_planned = 0
        staff_total_covered = 0
        processed_subject_ids = set()

        for a in assignments:
            subj = a.subject
            if not subj or not subj.is_active:
                continue
            processed_subject_ids.add(subj.id)
            
            subj_plans = plans_by_faculty_subject.get((staff_member.id, subj.id), [])
            if subj_plans:
                summary = _subject_summary(subj.id, plans)
                if summary:
                    if not summary.section:
                        summary.section = a.section
                    subject_summaries.append(summary)
                    staff_total_planned += summary.total_planned_periods
                    staff_total_covered += summary.total_covered_periods
            else:
                summary = schemas.SyllabusSubjectSummary(
                    subject_id=subj.id,
                    subject_name=subj.name,
                    course_code=subj.course_code,
                    semester=subj.semester,
                    section=a.section,
                    total_units=0,
                    completed_units=0,
                    total_planned_periods=0,
                    total_covered_periods=0,
                    overall_completion_percentage=0.0,
                    units=[],
                )
                subject_summaries.append(summary)

        plans_subject_ids = list(dict.fromkeys(p.subject_id for p in plans))
        for sid in plans_subject_ids:
            if sid not in processed_subject_ids:
                summary = _subject_summary(sid, plans)
                if summary:
                    subject_summaries.append(summary)
                    staff_total_planned += summary.total_planned_periods
                    staff_total_covered += summary.total_covered_periods

        dept_total_planned += staff_total_planned
        dept_total_covered += staff_total_covered

        staff_pct = round((staff_total_covered / staff_total_planned * 100), 1) if staff_total_planned > 0 else 0.0
        total_units = sum(s.total_units for s in subject_summaries)

        staff_overviews.append(schemas.SyllabusStaffOverview(
            staff_id=staff_member.id,
            staff_name=staff_member.name,
            department=staff_member.department,
            total_subjects=len(subject_summaries),
            total_units=total_units,
            total_planned_periods=staff_total_planned,
            total_covered_periods=staff_total_covered,
            overall_completion_percentage=staff_pct,
            subjects=subject_summaries,
        ))

    dept_pct = round((dept_total_covered / dept_total_planned * 100), 1) if dept_total_planned > 0 else 0.0

    return schemas.SyllabusHODOverviewResponse(
        academic_year=used_year,
        total_staff=len(all_staff),
        department_completion_percentage=dept_pct,
        staff_overviews=staff_overviews,
    )


# ─── HOD/Admin: specific staff member's progress ─────────────────────────────

@router.get(
    "/staff/{staff_id}",
    response_model=schemas.SyllabusStaffOverview,
    summary="Get Specific Staff Syllabus Progress",
    description="HOD/Admin view a specific staff member's complete syllabus coverage.",
)
async def get_staff_syllabus_progress(
    staff_id: int = Path(...),
    academic_year: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    _require_staff(current_user)
    role = current_user.role.name.lower() if current_user.role else ""

    # Regular staff can only view their own; HOD/admin can view anyone
    if staff_id != current_user.id and role not in HOD_ROLES:
        raise HTTPException(status_code=403, detail="You can only view your own progress")

    staff_res = await db.execute(select(models.Staff).where(models.Staff.id == staff_id))
    staff_member = staff_res.scalars().first()
    if not staff_member:
        raise HTTPException(status_code=404, detail="Staff not found")

    # 1. Fetch assignments
    assignment_stmt = (
        select(models.FacultySubjectAssignment)
        .where(models.FacultySubjectAssignment.faculty_id == staff_id)
    )
    if academic_year:
        assignment_stmt = assignment_stmt.where(
            (models.FacultySubjectAssignment.academic_year == academic_year) |
            (models.FacultySubjectAssignment.academic_year.is_(None))
        )
    assignment_result = await db.execute(assignment_stmt)
    assignments = assignment_result.scalars().all()
    for a in assignments:
        await db.refresh(a, ["subject"])

    # 2. Fetch syllabus plans
    stmt = (
        select(models.SyllabusPlan)
        .where(models.SyllabusPlan.faculty_id == staff_id)
        .order_by(models.SyllabusPlan.subject_id, models.SyllabusPlan.unit_number)
    )
    if academic_year:
        stmt = stmt.where(models.SyllabusPlan.academic_year == academic_year)

    result = await db.execute(stmt)
    plans: list[models.SyllabusPlan] = result.scalars().all()

    for plan in plans:
        await db.refresh(plan, ["subject", "faculty", "progress"])

    plans_by_subject = {}
    for plan in plans:
        plans_by_subject.setdefault(plan.subject_id, []).append(plan)

    subject_summaries = []
    total_planned = 0
    total_covered = 0
    processed_subject_ids = set()

    for a in assignments:
        subj = a.subject
        if not subj or not subj.is_active:
            continue
        processed_subject_ids.add(subj.id)
        
        subj_plans = plans_by_subject.get(subj.id, [])
        if subj_plans:
            summary = _subject_summary(subj.id, plans)
            if summary:
                if not summary.section:
                    summary.section = a.section
                subject_summaries.append(summary)
                total_planned += summary.total_planned_periods
                total_covered += summary.total_covered_periods
        else:
            summary = schemas.SyllabusSubjectSummary(
                subject_id=subj.id,
                subject_name=subj.name,
                course_code=subj.course_code,
                semester=subj.semester,
                section=a.section,
                total_units=0,
                completed_units=0,
                total_planned_periods=0,
                total_covered_periods=0,
                overall_completion_percentage=0.0,
                units=[],
            )
            subject_summaries.append(summary)

    for sid, subj_plans in plans_by_subject.items():
        if sid not in processed_subject_ids:
            summary = _subject_summary(sid, plans)
            if summary:
                subject_summaries.append(summary)
                total_planned += summary.total_planned_periods
                total_covered += summary.total_covered_periods

    pct = round((total_covered / total_planned * 100), 1) if total_planned > 0 else 0.0

    return schemas.SyllabusStaffOverview(
        staff_id=staff_member.id,
        staff_name=staff_member.name,
        department=staff_member.department,
        total_subjects=len(subject_summaries),
        total_units=sum(s.total_units for s in subject_summaries),
        total_planned_periods=total_planned,
        total_covered_periods=total_covered,
        overall_completion_percentage=pct,
        subjects=subject_summaries,
    )


# ─── Staff: delete a syllabus plan unit ──────────────────────────────────────

@router.delete(
    "/plan/{plan_id}",
    response_model=schemas.MessageResponse,
    summary="Delete Syllabus Plan Unit",
    description="Remove a syllabus unit. Faculty can delete their own; HOD/admin can delete any.",
)
async def delete_syllabus_plan(
    plan_id: int = Path(...),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    _require_staff(current_user)
    role = current_user.role.name.lower() if current_user.role else ""

    plan_res = await db.execute(select(models.SyllabusPlan).where(models.SyllabusPlan.id == plan_id))
    plan = plan_res.scalars().first()
    if not plan:
        raise HTTPException(status_code=404, detail="Syllabus plan unit not found")

    if plan.faculty_id != current_user.id and role not in HOD_ROLES:
        raise HTTPException(status_code=403, detail="You can only delete your own plans")

    await db.execute(delete(models.SyllabusPlan).where(models.SyllabusPlan.id == plan_id))
    await db.commit()
    return schemas.MessageResponse(message="Syllabus plan unit deleted successfully")
