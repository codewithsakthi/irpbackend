from datetime import date, datetime
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Path, Request, Body
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, func, case, insert
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
        # Flexible student filtering
        student_query = select(models.Student).filter(
            models.Student.program_id == subject.program_id,
            models.Student.current_semester.in_([subject.semester - 1, subject.semester, subject.semester + 1])
        )
        if a.section:
            student_query = student_query.filter(models.Student.section == a.section)
            
        students_res = await db.execute(student_query)
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
                pass_percentage=0.0,
                average_attendance=0.0
            ))
            continue

        assessments_res = await db.execute(
            select(models.StudentAssessment)
            .filter(models.StudentAssessment.subject_id == a.subject_id)
            .filter(models.StudentAssessment.student_id.in_(student_ids))
        )
        assessments = assessments_res.scalars().all()

        # Track per-student performance for a smarter Pass Rate
        student_stats = {} # student_id -> {sum, count, has_exam, passed_exam}
        total_m = 0.0
        count_marks = 0
        
        for m in assessments:
            if m.marks is not None:
                marks_val = float(m.marks)
                total_m += marks_val
                count_marks += 1
                
                sid = m.student_id
                if sid not in student_stats:
                    student_stats[sid] = {"sum": 0.0, "count": 0, "has_exam": False, "passed_exam": False}
                
                student_stats[sid]["sum"] += marks_val
                student_stats[sid]["count"] += 1
                
                if m.assessment_type.upper() == 'SEMESTER_EXAM':
                    student_stats[sid]["has_exam"] = True
                    if marks_val >= 50:
                        student_stats[sid]["passed_exam"] = True
                    
                    total_performance_acc += marks_val
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

        # Calculate smarter Pass Rate
        passed_students = 0
        for sid, stats in student_stats.items():
            if stats["has_exam"]:
                if stats["passed_exam"]:
                    passed_students += 1
            else:
                # Fallback: Is the aggregate CIT average >= 50?
                if stats["count"] > 0 and (stats["sum"] / stats["count"]) >= 50:
                    passed_students += 1
        
        pass_percentage = (passed_students / len(student_stats) * 100) if student_stats else 0.0
        average_marks = (total_m / count_marks) if count_marks else 0.0

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

    # Determine the section to filter by:
    # explicit query param > assignment.section
    effective_section = section or assignment.section

    # Fetch students filtered by program, semester AND section
    query = select(models.Student).filter(
        models.Student.program_id == subject.program_id,
        models.Student.current_semester.in_([subject.semester, subject.semester - 1])
    )
    if effective_section:
        query = query.filter(models.Student.section == effective_section)

    # Execute and fetch students
    result = await db.execute(query)
    students = result.scalars().all()

    # Fallback: if nothing found, try just program_id + section
    if not students:
        fallback_query = select(models.Student).filter(models.Student.program_id == subject.program_id)
        if effective_section:
            fallback_query = fallback_query.filter(models.Student.section == effective_section)
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
    "/subjects/{subject_id}/marks",
    response_model=List[schemas.StaffStudentMarkRow],
    summary="Get Students and Marks for Subject",
    description="Fetch students in the assigned section for a subject along with their aggregated CIT and Semester marks."
)
async def get_subject_marks(
    subject_id: int = Path(...),
    section: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    verify_staff_access(current_user)

    # Verify assignment
    assignment_res = await db.execute(
        select(models.FacultySubjectAssignment)
        .filter(models.FacultySubjectAssignment.faculty_id == current_user.id)
        .filter(models.FacultySubjectAssignment.subject_id == subject_id)
    )
    assignment = assignment_res.scalars().first()
    if not assignment:
        raise HTTPException(status_code=403, detail="Not assigned to this subject")

    # Fetch subject
    subject_res = await db.execute(select(models.Subject).filter(models.Subject.id == subject_id))
    subject = subject_res.scalars().first()
    
    effective_section = section or assignment.section

    # Fetch students
    student_query = select(models.Student).filter(
        models.Student.program_id == subject.program_id,
        models.Student.current_semester.in_([subject.semester, subject.semester - 1])
    )
    if effective_section:
        student_query = student_query.filter(models.Student.section == effective_section)
    
    students = (await db.execute(student_query)).scalars().all()
    student_ids = [s.id for s in students]

    if not student_ids:
        return []

    # Fetch assessments
    assessment_query = select(models.StudentAssessment).filter(
        models.StudentAssessment.subject_id == subject_id,
        models.StudentAssessment.student_id.in_(student_ids)
    )
    assessments = (await db.execute(assessment_query)).scalars().all()

    # Aggregate marks
    mark_map = {} # student_id -> {cit1, cit2, cit3, semester_exam}
    for a in assessments:
        sid = a.student_id
        if sid not in mark_map:
            mark_map[sid] = {"cit1": None, "cit2": None, "cit3": None, "semester_exam": None}
        
        atype = a.assessment_type.upper()
        if atype == "CIT1": mark_map[sid]["cit1"] = float(a.marks) if a.marks is not None else None
        elif atype == "CIT2": mark_map[sid]["cit2"] = float(a.marks) if a.marks is not None else None
        elif atype == "CIT3": mark_map[sid]["cit3"] = float(a.marks) if a.marks is not None else None
        elif atype == "SEMESTER_EXAM": mark_map[sid]["semester_exam"] = float(a.marks) if a.marks is not None else None

    # Final response
    result = []
    for s in students:
        marks = mark_map.get(s.id, {})
        result.append(schemas.StaffStudentMarkRow(
            student_id=s.id,
            roll_no=s.roll_no,
            name=s.name,
            cit1=marks.get("cit1"),
            cit2=marks.get("cit2"),
            cit3=marks.get("cit3"),
            semester_exam=marks.get("semester_exam")
        ))
    
    result.sort(key=lambda x: x.roll_no)
    return result


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
    assignment = assignment_res.scalars().first()
    is_substitute = assignment is None

    # Determine the section to use:
    # 1. Explicit section from the request body (frontend dropdown) takes highest priority
    # 2. Fall back to the assignment's section (for regular faculty)
    # 3. No filter for substitutes (they chose "All")
    section_filter = attendance_data.section or (assignment.section if (assignment and assignment.section) else None)

    # Fetch students by program + semester (+ section for regular faculty)
    student_query = (
        select(models.Student)
        .filter(models.Student.program_id == subject.program_id)
        .filter(models.Student.current_semester.in_([attendance_data.semester, attendance_data.semester - 1]))
    )
    if section_filter:
        student_query = student_query.filter(models.Student.section == section_filter)

    students_res = await db.execute(student_query)
    students = students_res.scalars().all()
    if not students:
        # Fallback to just program_id (+ section if applicable)
        fallback_query = select(models.Student).filter(models.Student.program_id == subject.program_id)
        if section_filter:
            fallback_query = fallback_query.filter(models.Student.section == section_filter)
        students_res = await db.execute(fallback_query)
        students = students_res.scalars().all()
    if not students:
        raise HTTPException(status_code=400, detail="No students found for this program/semester.")

    absentees_set = {roll.strip().upper() for roll in attendance_data.absentees if roll.strip()}
    od_set = {roll.strip().upper() for roll in getattr(attendance_data, 'od_list', []) if roll.strip()}

    # ── Bulk Attendance Operations ──
    now = datetime.now()
    
    # 1. Fetch existing attendance for this specific session
    existing_stmt = select(models.PeriodAttendance).where(
        models.PeriodAttendance.subject_id == attendance_data.subject_id,
        models.PeriodAttendance.date == attendance_data.date,
        models.PeriodAttendance.period == attendance_data.period,
        models.PeriodAttendance.student_id.in_([s.id for s in students])
    )
    existing_res = await db.execute(existing_stmt)
    existing_lookup = {r.student_id: r for r in existing_res.scalars().all()}

    to_insert = []
    to_update = []

    for student in students:
        roll = student.roll_no.strip().upper()
        status = 'P'
        if roll in absentees_set: status = 'A'
        elif roll in od_set: status = 'O'
        
        val_dict = {
            "student_id": student.id,
            "subject_id": attendance_data.subject_id,
            "semester": attendance_data.semester,
            "date": attendance_data.date,
            "period": attendance_data.period,
            "status": status,
            "marked_by_faculty_id": current_user.id,
            "is_substitute": is_substitute,
            "updated_at": now,
        }

        if student.id in existing_lookup:
            to_update.append(val_dict)
        else:
            to_insert.append(val_dict)

    # 2. Execute Batch Operations
    if to_insert:
        await db.execute(insert(models.PeriodAttendance), to_insert)
        
    if to_update:
        for upd in to_update:
            stmt = (
                update(models.PeriodAttendance)
                .where(models.PeriodAttendance.student_id == upd["student_id"])
                .where(models.PeriodAttendance.subject_id == upd["subject_id"])
                .where(models.PeriodAttendance.date == upd["date"])
                .where(models.PeriodAttendance.period == upd["period"])
                .values(
                    status=upd["status"],
                    marked_by_faculty_id=upd["marked_by_faculty_id"],
                    is_substitute=upd["is_substitute"],
                    updated_at=upd["updated_at"]
                )
            )
            await db.execute(stmt)

    await db.commit()
    sub_note = " (marked as substitute)" if is_substitute else ""

    # ── Compute stats for broadcast ───────────────────────────────────────────
    absentees_set_upper = {r.strip().upper() for r in attendance_data.absentees if r.strip()}
    od_set_upper = {r.strip().upper() for r in getattr(attendance_data, 'od_list', []) if r.strip()}
    present_count = sum(
        1 for s in students
        if s.roll_no.strip().upper() not in absentees_set_upper
        and s.roll_no.strip().upper() not in od_set_upper
    )
    absent_count = len([s for s in students if s.roll_no.strip().upper() in absentees_set_upper])
    od_count = len([s for s in students if s.roll_no.strip().upper() in od_set_upper])
    total_count = len(students)

    # ── Fetch faculty name ───────────────────────────────────────────────────
    faculty_res = await db.execute(select(models.Staff).filter(models.Staff.id == current_user.id))
    faculty = faculty_res.scalars().first()
    faculty_name = faculty.name if faculty else current_user.username

    # ── Notify admins + submitting staff (broadcast) ─────────────────────────
    await websocket.notify_attendance_broadcast(
        faculty_id=current_user.id,
        faculty_name=faculty_name,
        subject_name=subject.name,
        subject_code=subject.course_code,
        period=attendance_data.period,
        date=str(attendance_data.date),
        section=section_filter,
        semester=attendance_data.semester,
        present_count=present_count,
        absent_count=absent_count,
        od_count=od_count,
        total_count=total_count,
        is_substitute=is_substitute,
    )

    # ── Notify each student individually ─────────────────────────────────────
    for student in students:
        await websocket.notify_student_attendance_updated(
            student.roll_no,
            f"Attendance marked for {subject.course_code}, Period {attendance_data.period} on {attendance_data.date}"
        )

    return schemas.MessageResponse(message=f"Attendance submitted successfully{sub_note}")


@router.get(
    "/attendance/records",
    summary="Fetch Attendance Records for a Session",
    description="Returns per-student attendance status for a given subject, date, period, and optional section. Used for viewing and editing past attendance.",
)
async def get_attendance_records(
    subject_id: int = Query(..., description="Subject ID"),
    date: str = Query(..., description="Date in YYYY-MM-DD format"),
    period: int = Query(..., ge=1, le=7, description="Period number (1-7)"),
    section: Optional[str] = Query(None, description="Section filter (A/B)"),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    verify_staff_access(current_user)

    from datetime import date as dt_date
    query_date = dt_date.fromisoformat(date)

    # Fetch subject info for program/semester
    subject_res = await db.execute(select(models.Subject).filter(models.Subject.id == subject_id))
    subject = subject_res.scalars().first()
    if not subject:
        raise HTTPException(status_code=404, detail="Subject not found")

    # Fetch students for this subject (with optional section filter)
    student_query = (
        select(models.Student)
        .filter(models.Student.program_id == subject.program_id)
    )
    if section:
        student_query = student_query.filter(models.Student.section == section)

    students_res = await db.execute(student_query)
    students = students_res.scalars().all()

    # Build roll_no → student map
    student_map = {s.id: s for s in students}
    student_ids = list(student_map.keys())

    # Fetch existing attendance records for this session
    records_res = await db.execute(
        select(models.PeriodAttendance)
        .filter(
            models.PeriodAttendance.subject_id == subject_id,
            models.PeriodAttendance.date == query_date,
            models.PeriodAttendance.period == period,
            models.PeriodAttendance.student_id.in_(student_ids),
        )
    )
    records = {r.student_id: r for r in records_res.scalars().all()}

    result = []
    for student in students:
        rec = records.get(student.id)
        result.append({
            "roll_no": student.roll_no,
            "name": student.name,
            "section": student.section,
            "status": rec.status if rec else None,  # None means not yet marked
            "is_marked": rec is not None,
        })

    result.sort(key=lambda x: x["roll_no"])
    return result


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
