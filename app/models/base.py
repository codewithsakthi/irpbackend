from sqlalchemy import Column, Integer, String, Boolean, Date, ForeignKey, Numeric, ARRAY, CHAR, TIMESTAMP, text, Computed, BigInteger, CheckConstraint, UniqueConstraint, Index, func
from sqlalchemy.orm import relationship
from ..core.database import Base

class Role(Base):
    __tablename__ = "roles"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(50), unique=True, nullable=False)

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(100), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    role_id = Column(Integer, ForeignKey("roles.id"))
    is_initial_password = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP"))

    role = relationship("Role")
    refresh_tokens = relationship("RefreshToken", back_populates="user", cascade="all, delete-orphan")

class RefreshToken(Base):
    __tablename__ = "refresh_tokens"
    id = Column(Integer, primary_key=True, index=True)
    token_id = Column(String(255), unique=True, index=True, nullable=False) # JTI
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    expires_at = Column(TIMESTAMP, nullable=False)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    revoked_at = Column(TIMESTAMP, nullable=True)

    user = relationship("User", back_populates="refresh_tokens")

class Program(Base):
    __tablename__ = "programs"
    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(10), unique=True, nullable=False)
    name = Column(String(100), nullable=False)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP"))

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
    current_semester = Column(Integer, default=1)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP"))

    user = relationship("User")
    program = relationship("Program")
    assessments = relationship("StudentAssessment", back_populates="student", cascade="all, delete-orphan")

class Staff(Base):
    __tablename__ = "staff"
    id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    name = Column(String(255), nullable=False)
    email = Column(String(255), unique=True)
    department = Column(String(100))
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP"))

    user = relationship("User")
    assignments = relationship("FacultySubjectAssignment", back_populates="faculty")
    timetable = relationship("TimeTable", back_populates="faculty")

class Subject(Base):
    __tablename__ = "subjects"
    id = Column(Integer, primary_key=True, index=True)
    course_code = Column(String(20), nullable=False)
    name = Column(String(255), nullable=False)
    credits = Column(Integer, default=0)
    program_id = Column(Integer, ForeignKey("programs.id"))
    semester = Column(Integer)
    is_active = Column(Boolean, default=True, nullable=False)
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP"))

    __table_args__ = (
        UniqueConstraint('course_code', 'program_id', 'semester', name='uq_subject_identity'),
    )

    faculty_assignments = relationship("FacultySubjectAssignment", back_populates="subject")
    timetable_entries = relationship("TimeTable", back_populates="subject")

class FacultySubjectAssignment(Base):
    __tablename__ = "faculty_subject_assignments"
    id = Column(Integer, primary_key=True, index=True)
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
    batch = Column(String(20), nullable=False, index=True)  # e.g., "2024" 
    section = Column(String(20), nullable=False, index=True)  # A, B, C, D
    semester = Column(Integer, nullable=True)
    academic_year = Column(String(20), nullable=True)
    room_number = Column(String(50), nullable=True)
    # Temporarily removed: is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP"))

    __table_args__ = (
        UniqueConstraint('batch', 'section', 'day_of_week', 'period', 'academic_year', name='uq_timetable_slot'),
        CheckConstraint('day_of_week >= 1 AND day_of_week <= 7', name='check_day_of_week'),
        CheckConstraint('period >= 1 AND period <= 8', name='check_period'),
    )

    faculty = relationship("Staff", back_populates="timetable")
    subject = relationship("Subject", back_populates="timetable_entries")

class ContactInfo(Base):
    __tablename__ = "contact_info"
    student_id = Column(Integer, ForeignKey("students.id", ondelete="CASCADE"), primary_key=True)
    address = Column(String)
    pincode = Column(String)
    phone_primary = Column(String)
    phone_secondary = Column(String)
    phone_tertiary = Column(String)
    email = Column(String)
    city = Column(String)

    student = relationship("Student")

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
    student_id = Column(Integer, ForeignKey("students.id", ondelete="CASCADE"), nullable=False)
    school_name = Column(String)
    passing_year = Column(String)
    percentage = Column(Numeric)
    institution = Column(String)
    board_university = Column(String)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP"))

    student = relationship("Student")

class StudentAssessment(Base):
    """
    Unified assessment table replacing student_marks, semester_grades, and internal_marks.
    Handles various assessment types (CIT1, CIT2, CIT3, SEMESTER_EXAM) as rows.
    """
    __tablename__ = "student_assessments"
    id = Column(Integer, primary_key=True)
    student_id = Column(Integer, ForeignKey("students.id", ondelete="CASCADE"), nullable=False, index=True)
    subject_id = Column(Integer, ForeignKey("subjects.id", ondelete="CASCADE"), nullable=False, index=True)
    semester = Column(Integer, nullable=False)
    assessment_type = Column(String(20), nullable=False) # CIT1, CIT2, CIT3, SEMESTER_EXAM
    marks = Column(Numeric(5, 2))
    grade = Column(String(5), nullable=True)
    result_status = Column(String(10), nullable=True)
    attempt = Column(Integer, default=1)
    remarks = Column(String)
    is_final = Column(Boolean, default=False)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP"))
    updated_by = Column(Integer, ForeignKey("users.id"), nullable=True)

    student = relationship("Student", back_populates="assessments")
    subject = relationship("Subject")
    updator = relationship("User")

    __table_args__ = (
        UniqueConstraint('student_id', 'subject_id', 'semester', 'assessment_type', 'attempt', name='uq_student_assessment'),
        CheckConstraint("assessment_type IN ('CIT1', 'CIT2', 'CIT3', 'SEMESTER_EXAM', 'LAB', 'PROJECT')", name='chk_assessment_type'),
        Index('idx_assessment_student_sem', 'student_id', 'semester'),
        Index('uniq_final_assessment', 'student_id', 'subject_id', 'semester', 'assessment_type', unique=True, postgresql_where=text('is_final = true')),
    )

class CounselorDiary(Base):
    __tablename__ = "counselor_diary"
    meeting_id = Column(Integer, primary_key=True)
    student_id = Column(Integer, ForeignKey("students.id", ondelete="CASCADE"), nullable=False, index=True)
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

class ExtraCurricular(Base):
    __tablename__ = "extra_curricular"
    activity_id = Column(Integer, primary_key=True)
    student_id = Column(Integer, ForeignKey("students.id", ondelete="CASCADE"), nullable=False)
    category = Column(String)
    description = Column(String)
    year = Column(String)
    activity_type = Column(String)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP"))

    student = relationship("Student")

class PeriodAttendance(Base):
    """
    Per-period, per-subject attendance record.
    One row per (student, subject, date, period).
    """
    __tablename__ = "period_attendance"

    id = Column(Integer, primary_key=True, index=True)
    student_id = Column(Integer, ForeignKey("students.id", ondelete="CASCADE"), nullable=False)
    subject_id = Column(Integer, ForeignKey("subjects.id", ondelete="CASCADE"), nullable=False)
    date = Column(Date, nullable=False)
    period = Column(Integer, nullable=False)  # 1-7
    status = Column(CHAR(1), nullable=False, default='P') # P, A, L, O
    marked_by_faculty_id = Column(Integer, ForeignKey("staff.id"), nullable=True)
    is_substitute = Column(Boolean, default=False)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP"))
    updated_by = Column(Integer, ForeignKey("users.id"), nullable=True)

    __table_args__ = (
        UniqueConstraint('student_id', 'subject_id', 'date', 'period', name='uq_period_attendance'),
        CheckConstraint('period BETWEEN 1 AND 7', name='chk_period_range'),
        CheckConstraint("status IN ('P', 'A', 'L', 'O')", name='chk_pa_status'),
        Index('idx_pa_date_subject', 'date', 'subject_id'),
        Index('idx_pa_subject_date', 'subject_id', 'date'),
        Index('idx_pa_student_date', 'student_id', 'date'),
    )

    student = relationship("Student")
    subject = relationship("Subject")
    marked_by = relationship("Staff")

from sqlalchemy import JSON

class AuditLog(Base):
    __tablename__ = "audit_logs"
    id = Column(Integer, primary_key=True)
    table_name = Column(String(50), nullable=False)
    record_id = Column(BigInteger, nullable=False)
    action = Column(String(10), nullable=False) # INSERT, UPDATE, DELETE
    old_values = Column(JSON) # JSONB in PG
    new_values = Column(JSON) # JSONB in PG
    changed_by = Column(Integer, ForeignKey("users.id"), index=True)
    changed_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))

    user = relationship("User")

    __table_args__ = (
        Index('idx_audit_table_record', 'table_name', 'record_id'),
    )
