from __future__ import annotations
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Path, Request, Body
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, func, case
from sqlalchemy.orm import joinedload
import sqlalchemy as sa

from ...core import auth
from ...core.database import get_db
from ...models import base as models
from ...schemas import base as schemas
from ...core.limiter import limiter
from ...services.timetable_service import get_faculty_timetable
from ...services.assessment_service import AssessmentService
from . import websocket

router = APIRouter(tags=["Staff"])

# Roles permitted to access staff-level endpoints
ALLOWED_STAFF_ROLES = {"staff", "faculty", "hod", "director"}

def verify_staff_access(user: models.User):
    if not user.role or user.role.name.lower() not in ALLOWED_STAFF_ROLES:
        role_name = user.role.name if user.role else "None"
        raise HTTPException(
            status_code=403, 
            detail=f"Access forbidden: Staff role required. Current role: {role_name}"
        )

@router.get(
    "/schedule",
    response_model=List[schemas.StaffTimeTableEntry],
    summary="Get Staff Timetable",
    description="Return weekly timetable entries for the logged-in faculty. Falls back to the latest MCA II semester timetable if no DB rows exist.",
)
async def get_staff_schedule(
    section: Optional[str] = Query(None, description="Section filter, e.g., A or B"),
    semester: Optional[int] = Query(None, description="Semester filter"),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    verify_staff_access(current_user)

    timetable = await get_faculty_timetable(
        db=db,
        faculty_id=current_user.id,
        section=section,
        semester=semester,
    )
    return timetable

@router.get(
    "/me",
    response_model=schemas.StaffDashboardResponse,
    summary="Get Staff Dashboard Data",
    description="Retrieve dashboard data for the currently authenticated staff member, including assigned subjects."
)
@limiter.limit("20/minute")
async def get_staff_dashboard(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    verify_staff_access(current_user)

    # Fetch staff details
    result = await db.execute(select(models.Staff).filter(models.Staff.id == current_user.id))
    staff = result.scalars().first()
    if not staff:
        raise HTTPException(status_code=404, detail="Staff profile not found for this user account")

    # Fetch assigned subjects (normalized join: assignments + subjects + programs)
    result = await db.execute(
        select(models.FacultySubjectAssignment, models.Subject, models.Program)
        .join(models.Subject, models.FacultySubjectAssignment.subject_id == models.Subject.id)
        .outerjoin(models.Program, models.Subject.program_id == models.Program.id)
        .filter(
            models.FacultySubjectAssignment.faculty_id == staff.id,
            models.Subject.is_active == True  # Only show active subjects
        )
    )
    assignments = result.all()

    subjects = []
    total_students = 0
    total_pending_marks = 0
    total_performance_acc = 0.0
    performance_count = 0
    recent_updates = []

    for a, subject, _program in assignments:
        students_res = await db.execute(
            select(models.Student)
            .filter(models.Student.program_id == subject.program_id)
            .filter(models.Student.current_semester == subject.semester)
            .filter(models.Student.section == a.section)
        )
        students = students_res.scalars().all()
        count = len(students)
        total_students += count
        
        student_ids = [s.id for s in students]
        if not student_ids:
            subjects.append(schemas.StaffSubject(
                id=a.id,
                subject_id=a.subject_id,
                subject_name=subject.name,
                course_code=subject.course_code,
                semester=subject.semester,
                section=a.section,
                academic_year=a.academic_year,
                student_count=0,
                average_marks=0.0,
                pass_percentage=0.0
            ))
            continue

        assessments_res = await db.execute(
            select(models.StudentAssessment)
            .filter(models.StudentAssessment.subject_id == a.subject_id)
            .filter(models.StudentAssessment.student_id.in_(student_ids))
        )
        assessments = assessments_res.scalars().all()

        passed = 0
        total_m = 0.0
        count_final = 0
        
        for m in assessments:
            if m.assessment_type == 'SEMESTER_EXAM':
                count_final += 1
                if (m.marks or 0) >= 50:
                    passed += 1
                if m.marks is not None:
                    total_m += float(m.marks)
                    total_performance_acc += float(m.marks)
                    performance_count += 1
            
            if m.updated_at:
                stu = next((s for s in students if s.id == m.student_id), None)
                if stu:
                    recent_updates.append(schemas.RecentMarkUpdate(
                        subject_name=subject.name,
                        student_name=stu.name,
                        roll_no=stu.roll_no,
                        action=f"Updated {m.assessment_type}",
                        updated_at=m.updated_at
                    ))

        pass_percentage = (passed / count_final * 100) if count_final else 0.0
        average_marks = (total_m / count_final) if count_final else 0.0

        att_res = await db.execute(
            select(models.PeriodAttendance)
            .filter(models.PeriodAttendance.subject_id == a.subject_id)
            .filter(models.PeriodAttendance.student_id.in_(student_ids))
        )
        attendance_records = att_res.scalars().all()
        
        total_p = 0
        total_abs = 0
        for att in attendance_records:
            if att.status in ['P', 'O']:
                total_p += 1
            else:
                total_abs += 1
            
        avg_attendance = (total_p / (total_p + total_abs) * 100) if (total_p + total_abs) > 0 else 0.0

        subjects.append(schemas.StaffSubject(
            id=a.id,
            subject_id=a.subject_id,
            subject_name=subject.name,
            course_code=subject.course_code,
            semester=subject.semester,
            section=a.section,
            academic_year=a.academic_year,
            student_count=count,
            average_marks=round(average_marks, 2),
            pass_percentage=round(pass_percentage, 2),
            average_attendance=round(avg_attendance, 2)
        ))

    overall_avg = (total_performance_acc / performance_count) if performance_count > 0 else 0.0
    recent_updates.sort(key=lambda x: x.updated_at, reverse=True)
    top_5_updates = recent_updates[:5]

    return schemas.StaffDashboardResponse(
        staff_id=staff.id,
        name=staff.name,
        department=staff.department,
        subjects=subjects,
        total_students_handled=total_students,
        recent_marks_updates=top_5_updates,
        average_performance=round(overall_avg, 2),
        pending_marks_count=total_pending_marks
    )

@router.get(
    "/subjects/{subject_id}/students",
    response_model=List[schemas.AdminDirectoryStudent],
    summary="Get Students for Subject",
    description="List all students enrolled in a specific subject/section assigned to the staff."
)
async def get_subject_students(
    subject_id: int = Path(...),
    section: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    verify_staff_access(current_user)

    # Verify assignment
    result = await db.execute(
        select(models.FacultySubjectAssignment)
        .filter(models.FacultySubjectAssignment.faculty_id == current_user.id)
        .filter(models.FacultySubjectAssignment.subject_id == subject_id)
    )
    assignment = result.scalars().first()
    if not assignment:
        raise HTTPException(status_code=403, detail="Not assigned to this subject")

    # Fetch subject to get program/semester
    result = await db.execute(select(models.Subject).filter(models.Subject.id == subject_id))
    subject = result.scalars().first()

    # Fetch students. 
    # Use the subject's semester as target, but allow sem-1 mismatch (for transitions)
    query = select(models.Student).filter(
        models.Student.program_id == subject.program_id,
        models.Student.current_semester.in_([subject.semester, subject.semester - 1])
    )
    
    # Execute and fetch students
    result = await db.execute(query)
    students = result.scalars().all()

    # Fallback to just program_id if still nothing found
    if not students:
        fallback_query = select(models.Student).filter(models.Student.program_id == subject.program_id)
        if section:
            fallback_query = fallback_query.filter(models.Student.section == section)
        elif assignment.section:
            fallback_query = fallback_query.filter(models.Student.section == assignment.section)
        result = await db.execute(fallback_query)
        students = result.scalars().all()
    
    # Return as AdminDirectoryStudent (reusing existing schema for compatibility)
    res = []
    for s in students:
        res.append(schemas.AdminDirectoryStudent(
            roll_no=s.roll_no,
            name=s.name,
            batch=s.batch or "N/A",
            current_semester=s.current_semester,
            marks_count=0,
            attendance_count=0,
            attendance_percentage=0.0,
            average_grade_points=0.0,
            backlogs=0
        ))
    
    return res


@router.get(
    "/subjects/all",
    summary="Get All Subjects",
    description="Returns all subjects in the system. Used by substitutes to select any subject for attendance marking."
)
async def get_all_subjects(
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    verify_staff_access(current_user)

    result = await db.execute(
        select(models.Subject)
        .filter(models.Subject.is_active == True)  # Only return active subjects
        .order_by(models.Subject.semester, models.Subject.name)
    )
    subjects = result.scalars().all()

    return [
        {
            "id": s.id,
            "name": s.name,
            "course_code": s.course_code,
            "semester": s.semester,
            "credits": s.credits,
        }
        for s in subjects
    ]

@router.patch(
    "/marks",
    response_model=schemas.MessageResponse,
    summary="Update Student Marks",
    description="Batch update marks for multiple students in a subject."
)
async def update_marks(
    updates: List[schemas.StaffStudentAssessmentUpdate] = Body(...),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    verify_staff_access(current_user)

    await AssessmentService.apply_staff_assessment_updates(
        db=db,
        faculty_id=current_user.id,
        updates=updates,
        updated_by=current_user.id,
    )
    await db.commit()
    return schemas.MessageResponse(message="Marks updated successfully")

@router.post(
    "/attendance",
    response_model=schemas.MessageResponse,
    summary="Submit Attendance (Legacy)",
    description="Legacy endpoint — kept for backwards compatibility. Use /attendance/period for new submissions."
)
async def submit_attendance_legacy(
    attendance_data: schemas.StaffAttendanceCreate = Body(...),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    verify_staff_access(current_user)

    subject_res = await db.execute(select(models.Subject).filter(models.Subject.id == attendance_data.subject_id))
    subject = subject_res.scalars().first()
    if not subject:
        raise HTTPException(status_code=404, detail="Subject not found")

    students_res = await db.execute(
        select(models.Student)
        .filter(models.Student.program_id == subject.program_id)
        .filter(models.Student.current_semester == attendance_data.semester)
    )
    students = students_res.scalars().all()
    if not students:
        raise HTTPException(status_code=400, detail="No students found for this subject criteria.")

    absentees_set = {roll.strip().upper() for roll in attendance_data.absentees if roll.strip()}

    for student in students:
        is_absent = student.roll_no.strip().upper() in absentees_set
        status = 'A' if is_absent else 'P'

        # Check if record exists in period_attendance
        record_res = await db.execute(
            select(models.PeriodAttendance)
            .filter(models.PeriodAttendance.student_id == student.id)
            .filter(models.PeriodAttendance.subject_id == attendance_data.subject_id)
            .filter(models.PeriodAttendance.date == attendance_data.date)
            .filter(models.PeriodAttendance.period == attendance_data.period)
        )
        record = record_res.scalars().first()

        if record:
            await db.execute(
                update(models.PeriodAttendance)
                .where(models.PeriodAttendance.id == record.id)
                .values(status=status, marked_by_faculty_id=current_user.id)
            )
        else:
            db.add(models.PeriodAttendance(
                student_id=student.id,
                subject_id=attendance_data.subject_id,
                date=attendance_data.date,
                period=attendance_data.period,
                status=status,
                marked_by_faculty_id=current_user.id
            ))

    await db.commit()
    return schemas.MessageResponse(message="Attendance submitted successfully")


@router.post(
    "/attendance/period",
    response_model=schemas.MessageResponse,
    summary="Submit Period Attendance",
    description="Mark attendance for a specific subject and period. Any staff can mark any subject (substitute support). is_substitute is auto-detected."
)
async def submit_period_attendance(
    attendance_data: schemas.PeriodAttendanceCreate = Body(...),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    verify_staff_access(current_user)

    # Fetch subject
    subject_res = await db.execute(select(models.Subject).filter(models.Subject.id == attendance_data.subject_id))
    subject = subject_res.scalars().first()
    if not subject:
        raise HTTPException(status_code=404, detail="Subject not found")

    # Auto-detect substitute: check if this faculty is regularly assigned to this subject
    assignment_res = await db.execute(
        select(models.FacultySubjectAssignment)
        .filter(models.FacultySubjectAssignment.faculty_id == current_user.id)
        .filter(models.FacultySubjectAssignment.subject_id == attendance_data.subject_id)
    )
    is_substitute = assignment_res.scalars().first() is None

    # Fetch students in this program. 
    # Use the subject's semester as target, but allow sem-1 mismatch (for transitions)
    students_res = await db.execute(
        select(models.Student)
        .filter(models.Student.program_id == subject.program_id)
        .filter(models.Student.current_semester.in_([attendance_data.semester, attendance_data.semester - 1]))
    )
    students = students_res.scalars().all()
    if not students:
        # Fallback to just program_id if still nothing found
        students_res = await db.execute(
            select(models.Student).filter(models.Student.program_id == subject.program_id)
        )
        students = students_res.scalars().all()

    if not students:
        raise HTTPException(status_code=400, detail="No students found for this program/semester.")

    absentees_set = {roll.strip().upper() for roll in attendance_data.absentees if roll.strip()}
    od_set = {roll.strip().upper() for roll in getattr(attendance_data, 'od_list', []) if roll.strip()}

    for student in students:
        roll = student.roll_no.strip().upper()
        if roll in absentees_set:
            status = 'A'
        elif roll in od_set:
            status = 'O'
        else:
            status = 'P'

        # Try to find existing record for upsert
        existing_res = await db.execute(
            select(models.PeriodAttendance)
            .filter(models.PeriodAttendance.student_id == student.id)
            .filter(models.PeriodAttendance.subject_id == attendance_data.subject_id)
            .filter(models.PeriodAttendance.date == attendance_data.date)
            .filter(models.PeriodAttendance.period == attendance_data.period)
        )
        existing = existing_res.scalars().first()

        if existing:
            await db.execute(
                update(models.PeriodAttendance)
                .where(models.PeriodAttendance.id == existing.id)
                .values(
                    status=status,
                    marked_by_faculty_id=current_user.id,
                    is_substitute=is_substitute,
                    semester=attendance_data.semester,
                )
            )
        else:
            db.add(models.PeriodAttendance(
                student_id=student.id,
                subject_id=attendance_data.subject_id,
                date=attendance_data.date,
                period=attendance_data.period,
                status=status,
                marked_by_faculty_id=current_user.id,
                is_substitute=is_substitute,
                semester=attendance_data.semester,
            ))

    await db.commit()
    sub_note = " (marked as substitute)" if is_substitute else ""
    
    # Notify all students about the attendance update
    subject_name = subject.course_code if subject else "Unknown Subject"
    for student in students:
        await websocket.notify_student_attendance_updated(
            student.roll_no,
            f"Attendance marked for {subject_name}, Period {attendance_data.period} on {attendance_data.date}"
        )
    
    return schemas.MessageResponse(message=f"Attendance submitted successfully{sub_note}")


@router.get(
    "/attendance/today-summary",
    response_model=list[schemas.TodayStaffSummaryRow],
    summary="Today's Attendance Summary",
    description="Returns a summary of all periods this staff member has marked today."
)
async def get_today_summary(
    target_date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format. Defaults to today."),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    verify_staff_access(current_user)

    from datetime import date as dt_date
    import datetime
    query_date = dt_date.fromisoformat(target_date) if target_date else dt_date.today()

    result = await db.execute(
        select(
            models.PeriodAttendance.subject_id,
            models.PeriodAttendance.period,
            models.PeriodAttendance.is_substitute,
            func.count(models.PeriodAttendance.id).label("total"),
            func.sum(
                case((models.PeriodAttendance.status.in_(['P', 'O']), 1), else_=0)
            ).label("present"),
        )
        .filter(models.PeriodAttendance.marked_by_faculty_id == current_user.id)
        .filter(models.PeriodAttendance.date == query_date)
        .group_by(
            models.PeriodAttendance.subject_id,
            models.PeriodAttendance.period,
            models.PeriodAttendance.is_substitute,
        )
    )
    rows = result.all()

    # Fetch subject names
    subject_ids = list({r.subject_id for r in rows})
    subjects_res = await db.execute(select(models.Subject).filter(models.Subject.id.in_(subject_ids)))
    subjects_map = {s.id: s for s in subjects_res.scalars().all()}

    summary = []
    for row in rows:
        subject = subjects_map.get(row.subject_id)
        present = int(row.present or 0)
        total = int(row.total or 0)
        summary.append(schemas.TodayStaffSummaryRow(
            subject_name=subject.name if subject else f"Subject {row.subject_id}",
            course_code=subject.course_code if subject else "N/A",
            period=row.period,
            present_count=present,
            absent_count=total - present,
            total_students=total,
            is_substitute=row.is_substitute,
            attendance_percentage=(present / total * 100) if total > 0 else 0.0
        ))

    summary.sort(key=lambda x: x.period)
    return summary
