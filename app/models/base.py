from sqlalchemy import Column, Integer, SmallInteger, String, Boolean, Date, ForeignKey, Numeric, ARRAY, CHAR, TIMESTAMP, text, Computed, BigInteger, CheckConstraint, UniqueConstraint, Index, func, Time, Text
from sqlalchemy.orm import relationship
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.dialects.postgresql import JSONB

from ..core.database import Base

class Permission(Base):
    __tablename__ = "permissions"
    id = Column(Integer, primary_key=True)
    resource = Column(String(50), nullable=False)
    action = Column(String(20), nullable=False)
    description = Column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint("resource", "action", name="uq_permission_resource_action"),
    )

class RolePermission(Base):
    __tablename__ = "role_permissions"
    role_id = Column(Integer, ForeignKey("roles.id", ondelete="CASCADE"), primary_key=True)
    permission_id = Column(Integer, ForeignKey("permissions.id", ondelete="CASCADE"), primary_key=True)

class Role(Base):
    __tablename__ = "roles"
    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)

    permissions = relationship("Permission", secondary="role_permissions", backref="roles")

    __table_args__ = (
        UniqueConstraint("name", name="uq_role_name"),
    )

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(100), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    role_id = Column(Integer, ForeignKey("roles.id"))
    is_initial_password = Column(Boolean)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP"))

    role = relationship("Role")
    refresh_tokens = relationship("RefreshToken", back_populates="user", cascade="all, delete-orphan")

class RefreshToken(Base):
    __tablename__ = "refresh_tokens"
    id = Column(Integer, primary_key=True)
    token_id = Column(String(255), nullable=False) # JTI
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    expires_at = Column(TIMESTAMP, nullable=False)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    revoked_at = Column(TIMESTAMP, nullable=True)

    user = relationship("User", back_populates="refresh_tokens")

    __table_args__ = (
        UniqueConstraint("token_id", name="uq_refresh_tokens_token_id"),
    )

class Program(Base):
    __tablename__ = "programs"
    id = Column(Integer, primary_key=True)
    code = Column(String(10), nullable=False)
    name = Column(String(100), nullable=False)
    degree_type = Column(String(20), nullable=False, default="undergraduate")
    duration_years = Column(SmallInteger, nullable=False, default=4)
    total_semesters = Column(SmallInteger, nullable=False, default=8)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP"))

    __table_args__ = (
        UniqueConstraint("code", name="uq_program_code"),
    )

class Student(Base):
    __tablename__ = "students"
    id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    roll_no = Column(String(20), unique=True, nullable=False)
    reg_no = Column(String(20), unique=True)
    name = Column(String(255), nullable=False)
    dob = Column(Date, nullable=False)
    email = Column(String(255))
    batch = Column(String(20))
    section = Column(String(10))
    program_id = Column(Integer, ForeignKey("programs.id"))
    current_semester = Column(Integer)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP"))

    # Soft delete support
    is_deleted = Column(Boolean, default=False, nullable=False, index=True)
    deleted_at = Column(TIMESTAMP, nullable=True)

    # Consolidated Contact Info
    address = Column(String, nullable=True)
    pincode = Column(String, nullable=True)
    phone_primary = Column(String, nullable=True)
    phone_secondary = Column(String, nullable=True)
    phone_tertiary = Column(String, nullable=True)
    city = Column(String, nullable=True)

    user = relationship("User", foreign_keys=[id])
    program = relationship("Program")
    assessments = relationship("StudentAssessment", back_populates="student", cascade="all, delete-orphan")

    __table_args__ = (
        CheckConstraint("current_semester >= 1", name="chk_valid_semester"),
        Index('idx_students_batch_sem_section', 'batch', 'current_semester', 'section'),
        Index('idx_students_roll_no', 'roll_no'),
    )

class Staff(Base):
    __tablename__ = "staff"
    id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    name = Column(String(255), nullable=False)
    email = Column(String(255), unique=True)
    department = Column(String(100))
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP"))

    # Soft delete support
    is_deleted = Column(Boolean, default=False, nullable=False, index=True)
    deleted_at = Column(TIMESTAMP, nullable=True)

    user = relationship("User", foreign_keys=[id])
    assignments = relationship("FacultySubjectAssignment", back_populates="faculty")
    timetable = relationship("TimeTable", back_populates="faculty")

class StaffProfile(Base):
    __tablename__ = "staff_profile"
    staff_id = Column(Integer, ForeignKey("staff.id", ondelete="CASCADE"), primary_key=True)
    designation = Column(String(100))
    years_of_experience = Column(Numeric(4, 1))
    date_of_joining = Column(Date)
    employee_type = Column(String(30))
    specialisation = Column(String(255))
    google_scholar_url = Column(String(500))
    orcid_id = Column(String(50))
    bio = Column(Text)
    profile_photo_url = Column(String(500))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP"))

    staff = relationship("Staff")

class StaffQualification(Base):
    __tablename__ = "staff_qualifications"
    id = Column(Integer, primary_key=True)
    staff_id = Column(Integer, ForeignKey("staff.id", ondelete="CASCADE"), nullable=False)
    degree = Column(String(100))
    specialisation = Column(String(255))
    institution = Column(String(255))
    year_of_passing = Column(Integer)
    grade_or_percentage = Column(String(20))

    staff = relationship("Staff")

class StaffExperience(Base):
    __tablename__ = "staff_experience"
    id = Column(Integer, primary_key=True)
    staff_id = Column(Integer, ForeignKey("staff.id", ondelete="CASCADE"), nullable=False)
    organisation = Column(String(255))
    role = Column(String(100))
    start_date = Column(Date)
    end_date = Column(Date)
    is_current = Column(Boolean, server_default=text("false"))
    experience_type = Column(String(30))

    staff = relationship("Staff")

class StaffCertification(Base):
    __tablename__ = "staff_certifications"
    id = Column(Integer, primary_key=True)
    staff_id = Column(Integer, ForeignKey("staff.id", ondelete="CASCADE"), nullable=False)
    certification_name = Column(String(255))
    issuing_body = Column(String(255))
    issue_date = Column(Date)
    expiry_date = Column(Date)
    credential_id = Column(String(100))

    staff = relationship("Staff")

class StaffPublication(Base):
    __tablename__ = "staff_publications"
    id = Column(Integer, primary_key=True)
    staff_id = Column(Integer, ForeignKey("staff.id", ondelete="CASCADE"), nullable=False)
    title = Column(Text, nullable=False)
    publication_type = Column(String(30))
    journal_or_conf = Column(String(255))
    year = Column(Integer)
    doi_or_url = Column(String(500))
    is_indexed = Column(Boolean, server_default=text("false"))

    staff = relationship("Staff")

class StaffAchievement(Base):
    __tablename__ = "staff_achievements"
    id = Column(Integer, primary_key=True)
    staff_id = Column(Integer, ForeignKey("staff.id", ondelete="CASCADE"), nullable=False)
    title = Column(String(255))
    category = Column(String(50))
    awarded_by = Column(String(255))
    year = Column(Integer)
    description = Column(Text)

    staff = relationship("Staff")

class Subject(Base):
    __tablename__ = "subjects"
    id = Column(Integer, primary_key=True, index=True)
    course_code = Column(String(20), nullable=False)
    name = Column(String(255), nullable=False)
    credits = Column(Numeric(4, 2))
    program_id = Column(Integer, ForeignKey("programs.id"))
    semester = Column(Integer)
    is_active = Column(Boolean, default=True, nullable=False)
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP"))

    # Soft delete support
    is_deleted = Column(Boolean, default=False, nullable=False, index=True)
    deleted_at = Column(TIMESTAMP, nullable=True)
    
    # Threshold configuration for hybrid performance evaluation
    pass_threshold = Column(Numeric(5, 2), default=50.0, nullable=False, comment="Minimum marks to pass (default: 50)")
    target_average = Column(Numeric(5, 2), nullable=True, comment="Target average for good performance")
    percentile_excellent = Column(Numeric(5, 2), default=85.0, nullable=False, comment="Minimum percentile for 'Excellent' (default: 85)")
    percentile_good = Column(Numeric(5, 2), default=60.0, nullable=False, comment="Minimum percentile for 'Good' (default: 60)")
    percentile_average = Column(Numeric(5, 2), default=30.0, nullable=False, comment="Minimum percentile for 'Average' (default: 30)")
    
    __table_args__ = (
        UniqueConstraint('course_code', 'program_id', 'semester', name='uq_subject_identity'),
        CheckConstraint("pass_threshold >= 0 AND pass_threshold <= 100", name='chk_pass_threshold_range'),
        CheckConstraint("target_average IS NULL OR (target_average >= 0 AND target_average <= 100)", name='chk_target_average_range'),
        CheckConstraint("percentile_excellent >= percentile_good AND percentile_good >= percentile_average AND percentile_average >= 0 AND percentile_excellent <= 100", name='chk_percentile_hierarchy'),
        Index('idx_subjects_active_semester', 'is_active', 'semester', postgresql_where=text('is_active = true')),
        Index('idx_subjects_course_code_lower', func.lower(course_code)),
    )

    faculty_assignments = relationship("FacultySubjectAssignment", back_populates="subject")
    timetable_entries = relationship("TimeTable", back_populates="subject")

class FacultySubjectAssignment(Base):
    __tablename__ = "faculty_subject_assignments"
    id = Column(Integer, primary_key=True)
    faculty_id = Column(Integer, ForeignKey("staff.id"), nullable=False)
    subject_id = Column(Integer, ForeignKey("subjects.id"), nullable=False)
    academic_year = Column(String(20))
    section = Column(String(20))
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))

    faculty = relationship("Staff", back_populates="assignments")
    subject = relationship("Subject", back_populates="faculty_assignments")

    __table_args__ = (
        UniqueConstraint('faculty_id', 'subject_id', 'section', 'academic_year', name='uniq_faculty_assignment'),
    )

class TimeTable(Base):
    __tablename__ = "timetable"
    id = Column(Integer, primary_key=True, index=True)
    day_of_week = Column(Integer, nullable=False) # 1 for Monday, 7 for Sunday
    period = Column(Integer, nullable=False) # 1 to 8 (renamed from hour for clarity)
    subject_id = Column(Integer, ForeignKey("subjects.id"), nullable=True)  # Allow null for breaks
    faculty_id = Column(Integer, ForeignKey("staff.id"), nullable=True)  # Allow null for breaks
    batch = Column(String(20), nullable=True)  # e.g., "2024"
    section = Column(String(20), nullable=False)  # A, B, C, D
    semester = Column(Integer, nullable=True)
    academic_year = Column(String(20), nullable=True)
    room_number = Column(String(50), nullable=True)
    start_time = Column(Time, nullable=True)
    end_time = Column(Time, nullable=True)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP"))

    __table_args__ = (
        UniqueConstraint('batch', 'section', 'day_of_week', 'period', 'semester', 'academic_year', name='uq_timetable_slot'),
        CheckConstraint('start_time < end_time', name='chk_timetable_times'),
    )

    faculty = relationship("Staff", back_populates="timetable")
    subject = relationship("Subject", back_populates="timetable_entries")

class FamilyDetail(Base):
    __tablename__ = "family_details"
    student_id = Column(Integer, ForeignKey("students.id", ondelete="CASCADE"), primary_key=True)
    parent_guardian_name = Column(String)
    occupation = Column(String)
    parent_phone = Column(String)
    father_name = Column(String)
    mother_name = Column(String)
    parent_occupation = Column(String)
    parent_address = Column(String)
    parent_email = Column(String)
    emergency_contact_name = Column(String)
    emergency_contact_phone = Column(String)
    emergency_contact_relation = Column(String)
    emergency_contact_address = Column(String)
    emergency_contact_email = Column(String)

    student = relationship("Student")

class PreviousAcademic(Base):
    __tablename__ = "previous_academics"
    id = Column(Integer, primary_key=True)
    student_id = Column(Integer, ForeignKey("students.id"), nullable=False)
    passing_year = Column(String)
    percentage = Column(Numeric)
    institution = Column(String, nullable=True)
    board_university = Column(String)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP"))

    student = relationship("Student")

    @hybrid_property
    def school_name(self):
        return self.institution

    @school_name.setter
    def school_name(self, value):
        self.institution = value

class StudentAssessment(Base):
    """
    Unified assessment table replacing student_marks, semester_grades, and internal_marks.
    Handles various assessment types (CIT1, CIT2, CIT3, SEMESTER_EXAM) as rows.
    """
    __tablename__ = "student_assessments"
    id = Column(Integer, primary_key=True)
    student_id = Column(Integer, ForeignKey("students.id", ondelete="CASCADE"), nullable=False)
    subject_id = Column(Integer, ForeignKey("subjects.id", ondelete="CASCADE"), nullable=False)
    semester = Column(Integer, nullable=False)
    assessment_type = Column(String(20), nullable=False) # CIT1, CIT2, CIT3, SEMESTER_EXAM
    marks = Column(Numeric(5, 2))
    grade = Column(String(5), nullable=True)
    result_status = Column(String(10), nullable=True)
    attempt = Column(Integer, default=1)
    remarks = Column(Text)
    is_final = Column(Boolean, default=False)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP"))
    updated_by = Column(Integer, ForeignKey("users.id"), nullable=True)

    student = relationship("Student", back_populates="assessments")
    subject = relationship("Subject")
    updator = relationship("User")

    __table_args__ = (
        CheckConstraint("assessment_type IN ('CIT1', 'CIT2', 'CIT3', 'SEMESTER_EXAM', 'LAB', 'PROJECT')", name='chk_assessment_type'),
        Index('idx_assessment_student_sem', 'student_id', 'semester'),
        Index('idx_sa_student_type', 'student_id', 'assessment_type', postgresql_where=text('is_final = true')),
        Index('uq_sa_final', 'student_id', 'subject_id', 'assessment_type', unique=True, postgresql_where=text('is_final = true')),
    )

class CounselorDiary(Base):
    __tablename__ = "counselor_diary"
    meeting_id = Column(Integer, primary_key=True)
    student_id = Column(Integer, ForeignKey("students.id"), nullable=False)
    semester = Column(Integer)
    meeting_date = Column(Date)
    remark_category = Column(String)
    remarks = Column(String)
    action_planned = Column(String)
    follow_up_date = Column(Date)
    counselor_id = Column(Integer, ForeignKey("staff.id"), nullable=True)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP"))

    student = relationship("Student")
    counselor = relationship("Staff")

    __table_args__ = (
        Index('idx_counselor_diary_student', 'student_id'),
    )

class ExtraCurricular(Base):
    __tablename__ = "extra_curricular"
    activity_id = Column(Integer, primary_key=True)
    student_id = Column(Integer, ForeignKey("students.id"), nullable=False)
    category = Column(String)
    description = Column(String)
    year = Column(String)
    activity_type = Column(String)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP"))

    student = relationship("Student")

    __table_args__ = (
        Index('idx_extra_curricular_student', 'student_id'),
    )

class PeriodAttendance(Base):
    """
    Per-period, per-subject attendance record.
    One row per (student, subject, date, period).
    """
    __tablename__ = "period_attendance"

    id = Column(Integer, primary_key=True, index=True)
    student_id = Column(Integer, ForeignKey("students.id"), nullable=False)
    subject_id = Column(Integer, ForeignKey("subjects.id"), nullable=False)
    date = Column(Date, nullable=False)
    period = Column(Integer, nullable=False)  # 1-7
    status = Column(CHAR(1), nullable=False, default='P') # P, A, L, O
    marked_by_faculty_id = Column(Integer, ForeignKey("staff.id"), nullable=True)
    is_substitute = Column(Boolean, default=False)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP"))
    semester = Column(Integer, nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"), nullable=True)

    __table_args__ = (
        UniqueConstraint('student_id', 'subject_id', 'date', 'period', name='uq_period_attendance'),
        CheckConstraint('period BETWEEN 1 AND 7', name='chk_period_range'),
        CheckConstraint("status IN ('P', 'A', 'L', 'O')", name='chk_pa_status'),
        Index('idx_pa_date_subject', 'date', 'subject_id'),
        Index('idx_period_attendance_student_date_sem', 'student_id', 'date', 'semester'),
        Index('idx_pa_subject_date', 'subject_id', 'date'),
        Index('idx_pa_student_date', 'student_id', 'date'),
    )

    student = relationship("Student")
    subject = relationship("Subject")
    marked_by = relationship("Staff")

from sqlalchemy import JSON

class AuditLog(Base):
    __tablename__ = "audit_logs"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    table_name = Column(String(50), nullable=False)
    record_id = Column(BigInteger, nullable=False)
    action = Column(String(10), nullable=False) # INSERT, UPDATE, DELETE
    old_values = Column(JSONB)
    new_values = Column(JSONB)
    changed_by = Column(Integer, ForeignKey("users.id"), index=True)
    changed_at = Column(TIMESTAMP, primary_key=True, server_default=text("CURRENT_TIMESTAMP"))

    user = relationship("User")

    __table_args__ = (
        Index('idx_audit_table_record', 'table_name', 'record_id'),
    )

class StudentCapabilityScore(Base):
    __tablename__ = "student_capability_scores"
    student_id = Column(Integer, ForeignKey("students.id", ondelete="CASCADE"), primary_key=True, index=True)
    academic_score = Column(Numeric(5, 2))
    communication_score = Column(Numeric(5, 2))
    leadership_score = Column(Numeric(5, 2))
    technical_score = Column(Numeric(5, 2))
    creativity_score = Column(Numeric(5, 2))
    sports_score = Column(Numeric(5, 2))
    discipline_score = Column(Numeric(5, 2))
    consistency_score = Column(Numeric(5, 2))
    placement_score = Column(Numeric(5, 2))
    growth_score = Column(Numeric(5, 2))
    spi_score = Column(Numeric(5, 2))
    profile_type = Column(String(100), nullable=True)
    placement_probability = Column(Numeric(5, 2))
    confidence_score = Column(Numeric(4, 2))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP"))
    computed_at = Column(TIMESTAMP)

    student = relationship("Student", backref="capability_scores", uselist=False)

class StudentGrowthHistory(Base):
    __tablename__ = "student_growth_history"
    id = Column(Integer, primary_key=True, index=True)
    student_id = Column(Integer, ForeignKey("students.id", ondelete="CASCADE"), nullable=False, index=True)
    semester = Column(Integer, nullable=False)
    spi_score = Column(Numeric(5, 2))
    growth_delta = Column(Numeric(5, 2))
    generated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))

    student = relationship("Student", backref="growth_history")

    __table_args__ = (
        UniqueConstraint('student_id', 'semester', name='uq_student_semester_growth'),
    )

class StudentSubjectEnrollment(Base):
    __tablename__ = "student_subject_enrollment"
    id = Column(Integer, primary_key=True)
    student_id = Column(Integer, ForeignKey("students.id", ondelete="CASCADE"), nullable=False, index=True)
    subject_id = Column(Integer, ForeignKey("subjects.id", ondelete="CASCADE"), nullable=False, index=True)
    semester = Column(Integer, nullable=False)
    academic_year = Column(String(20), nullable=False)
    status = Column(String(20), default="active", nullable=False) # active, dropped, completed
    enrolled_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))

    student = relationship("Student")
    subject = relationship("Subject")

    __table_args__ = (
        UniqueConstraint('student_id', 'subject_id', 'academic_year', name='uq_student_subject_enrollment'),
        CheckConstraint("status IN ('active', 'dropped', 'completed')", name='chk_student_subject_enrollment_status'),
    )

class Notification(Base):
    __tablename__ = "notifications"
    id = Column(Integer, primary_key=True)
    recipient_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="References base users.id. Since students and staff tables share their primary key with users.id (joined-table inheritance), they are also reached via this base ID."
    )
    title = Column(String(255), nullable=False)
    content = Column(Text, nullable=False)
    channel = Column(String(50), nullable=False) # SYSTEM, EMAIL, SMS
    status = Column(String(50), nullable=False) # unread, read, sent, failed
    priority = Column(String(20), nullable=False) # low, normal, high
    sender_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    read_at = Column(TIMESTAMP, nullable=True)
    metadata_json = Column(JSON, nullable=True)

    recipient = relationship("User", foreign_keys=[recipient_id])
    sender = relationship("User", foreign_keys=[sender_id])

    __table_args__ = (
        Index('idx_recipient_status_channel', 'recipient_id', 'status', 'channel'),
    )

class AcademicCalendar(Base):
    __tablename__ = "academic_calendar"
    id = Column(Integer, primary_key=True, index=True)
    program_id = Column(Integer, ForeignKey("programs.id", ondelete="CASCADE"), nullable=True)
    academic_year = Column(String(20), nullable=False)
    semester = Column(SmallInteger, nullable=False)
    event_type = Column(String(30), nullable=False)
    title = Column(String(255), nullable=False)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)
    description = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))

    program = relationship("Program")

    __table_args__ = (
        CheckConstraint("end_date >= start_date", name="chk_date_order"),
        Index("idx_acad_cal_year_sem", "academic_year", "semester"),
        Index("idx_acad_cal_type_start", "event_type", "start_date"),
    )


class SyllabusPlan(Base):
    """
    Defines the planned syllabus units for a faculty-subject assignment.
    One row per unit per subject per faculty per academic year.
    """
    __tablename__ = "syllabus_plans"

    id = Column(Integer, primary_key=True, index=True)
    subject_id = Column(Integer, ForeignKey("subjects.id", ondelete="CASCADE"), nullable=False)
    faculty_id = Column(Integer, ForeignKey("staff.id", ondelete="CASCADE"), nullable=False)
    academic_year = Column(String(20), nullable=False)
    section = Column(String(20), nullable=True)
    unit_number = Column(Integer, nullable=False)
    unit_title = Column(String(255), nullable=False)
    total_periods = Column(Integer, nullable=False, default=0)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP"))

    subject = relationship("Subject")
    faculty = relationship("Staff")
    progress = relationship("SyllabusProgress", back_populates="plan", uselist=False, cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("subject_id", "faculty_id", "academic_year", "section", "unit_number",
                         name="uq_syllabus_plan_unit"),
        Index("idx_syllabus_plan_faculty", "faculty_id"),
        Index("idx_syllabus_plan_subject", "subject_id"),
        CheckConstraint("total_periods >= 0", name="chk_syllabus_total_periods"),
    )


class SyllabusProgress(Base):
    """
    Tracks actual periods covered for a syllabus plan unit.
    Updated by the faculty member; viewed by HOD/admin.
    """
    __tablename__ = "syllabus_progress"

    id = Column(Integer, primary_key=True, index=True)
    plan_id = Column(Integer, ForeignKey("syllabus_plans.id", ondelete="CASCADE"), nullable=False, unique=True)
    faculty_id = Column(Integer, ForeignKey("staff.id", ondelete="CASCADE"), nullable=False)
    covered_periods = Column(Integer, nullable=False, default=0)
    notes = Column(Text, nullable=True)
    last_updated = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP"))
    updated_by = Column(Integer, ForeignKey("users.id"), nullable=True)

    plan = relationship("SyllabusPlan", back_populates="progress")
    faculty = relationship("Staff")
    updater = relationship("User")

    __table_args__ = (
        CheckConstraint("covered_periods >= 0", name="chk_syllabus_covered_periods"),
        Index("idx_syllabus_progress_faculty", "faculty_id"),
    )


class Achievement(Base):
    """
    User achievements, awards, certifications, or journal publications.
    Logged by any user role (student, staff, admin) and broadcasted to everyone.
    """
    __tablename__ = "achievements"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    achievement_type = Column(String(50), nullable=False, default="achievement") # achievement, journal, publication, award, certification
    date_achieved = Column(Date, nullable=True)
    attachment_url = Column(String(500), nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())

    user = relationship("User")


class PushSubscription(Base):
    """
    Stores PWA Web Push subscriptions for users to deliver native background push notifications.
    """
    __tablename__ = "push_subscriptions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    endpoint = Column(Text, nullable=False, unique=True)
    p256dh = Column(String(500), nullable=False)
    auth = Column(String(500), nullable=False)
    created_at = Column(TIMESTAMP, server_default=func.now())

    user = relationship("User")


