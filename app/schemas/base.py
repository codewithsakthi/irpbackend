from __future__ import annotations
import datetime as dt
from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, EmailStr, Field


class RoleBase(BaseModel):
    name: str


class RoleCreate(RoleBase):
    pass


class Role(RoleBase):
    id: int

    class Config:
        from_attributes = True


class UserBase(BaseModel):
    username: str


class UserCreate(UserBase):
    password: str
    role_id: int


class User(UserBase):
    id: int
    role_id: int
    is_initial_password: bool
    created_at: datetime

    class Config:
        from_attributes = True


class CurrentUser(User):
    role: Optional[str] = None
    name: Optional[str] = None
    email: Optional[EmailStr] = None
    roll_no: Optional[str] = None
    reg_no: Optional[str] = None
    batch: Optional[str] = None
    current_semester: Optional[int] = None
    program_name: Optional[str] = None
    program_code: Optional[str] = None
    rank: Optional[int] = None


class ProfileUpdate(BaseModel):
    name: str
    email: Optional[EmailStr] = None
    batch: Optional[str] = None


class PasswordChangeRequest(BaseModel):
    current_password: str
    new_password: str


class MessageResponse(BaseModel):
    message: str


class BulkImportStudentResult(BaseModel):
    roll_no: str
    name: Optional[str] = None
    username: str
    initial_password: Optional[str] = None
    file_name: str


class BulkImportError(BaseModel):
    file_name: str
    error: str


class BulkImportResponse(BaseModel):
    message: str
    imported_count: int
    error_count: int
    imported_students: List[BulkImportStudentResult] = Field(default_factory=list)
    errors: List[BulkImportError] = Field(default_factory=list)


class AdminStudentCredential(BaseModel):
    roll_no: str
    username: str
    has_account: bool = True
    is_initial_password: bool
    initial_password_hint: Optional[str] = None
    dob_masked: Optional[str] = None
    note: Optional[str] = None


class StudentBase(BaseModel):
    roll_no: str
    reg_no: Optional[str]
    name: str
    dob: date
    email: Optional[EmailStr]
    batch: Optional[str]
    program_id: Optional[int]
    current_semester: Optional[int]


class StudentCreate(StudentBase):
    user_id: int


class Student(StudentBase):
    id: int

    class Config:
        from_attributes = True


class AssessmentBase(BaseModel):
    semester: int
    assessment_type: str = Field(..., pattern='^(CIT1|CIT2|CIT3|SEMESTER_EXAM)$')
    marks: Optional[float] = None
    attempt: int = 1
    remarks: Optional[str] = None
    is_final: bool = False

class AssessmentCreate(AssessmentBase):
    student_id: int
    subject_id: int

class AssessmentResponse(AssessmentBase):
    id: int
    student_id: int
    subject_id: int
    created_at: datetime
    updated_at: datetime
    updated_by: Optional[int] = None
    subject: Optional[Subject] = None

    class Config:
        from_attributes = True


class Subject(BaseModel):
    id: int
    course_code: str
    name: str
    credits: float = 0.0
    semester: Optional[int] = None
    is_active: bool = True

    class Config:
        from_attributes = True


class StudentAssessmentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    semester: int
    subject: Optional[Subject] = None
    assessment_type: str
    marks: Optional[float] = None
    attempt: int = 1
    remarks: Optional[str] = None
    is_final: bool = False
    created_at: datetime

class StaffAttendanceCreate(BaseModel):
    """Legacy schema — kept for backwards compatibility."""
    subject_id: int
    date: date
    hour: int = Field(ge=1, le=7)
    absentees: List[str] = Field(default_factory=list)
    section: str = "A"
    semester: int


class PeriodAttendanceCreate(BaseModel):
    """New schema for per-period, per-subject attendance submission."""
    subject_id: int
    date: date
    period: int = Field(ge=1, le=7, description="Period number (1-7)")
    absentees: List[str] = Field(default_factory=list, description="Roll numbers of absent students")
    od_list: List[str] = Field(default_factory=list, description="Roll numbers of 'On Duty' students")
    semester: int
    section: Optional[str] = Field(default=None, description="Section filter (e.g. 'A' or 'B'). If provided, overrides the assignment's section.")


class SubjectAttendanceRow(BaseModel):
    """One subject's attendance stats for the daily combined report."""
    subject_id: int
    subject_name: str
    course_code: str
    period: int
    total_students: int
    present_count: int
    absent_count: int
    attendance_percentage: float
    marked_by_faculty_name: Optional[str] = None
    is_substitute: bool = False


class DailyAttendanceReport(BaseModel):
    """Combined daily report grouping all subjects and periods."""
    date: date
    rows: List[SubjectAttendanceRow] = Field(default_factory=list)
    total_periods_marked: int = 0
    summary: Optional[str] = None


class TodayStaffSummaryRow(BaseModel):
    """Summary row for staff — shows what this staff has marked today."""
    subject_name: str
    course_code: str
    period: int
    present_count: int
    absent_count: int
    total_students: int
    is_substitute: bool


class AttendanceResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    student_id: int
    semester: int
    present_periods: int
    absent_periods: int
    leave_periods: int
    total_periods: int
    attendance_percentage: float
    # Student UI expects a day-wise display with per-period status chips.
    date: Optional[dt.date] = None
    status_array: List[str] = Field(default_factory=list)
    total_present: Optional[int] = None
    total_hours: Optional[int] = None

class PaginatedAttendance(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    items: List[AttendanceResponse]
    total: int
    page: int
    size: int
    pages: int
    summary: Optional[AttendanceInsight] = None

class Program(BaseModel):
    id: int
    code: str
    name: str

    class Config:
        from_attributes = True

class ReportCardMark(BaseModel):
    model_config = ConfigDict(extra='forbid')

    semester: int
    subject: Subject

    cit1: Optional[float] = None
    cit2: Optional[float] = None
    cit3: Optional[float] = None
    sem_exam: Optional[float] = None
    lab: Optional[float] = None
    project: Optional[float] = None

    internal_marks: Optional[float] = None
    total_marks: Optional[float] = None
    grade: Optional[str] = None
    result_status: Optional[str] = None
    attempt: Optional[int] = None
    remarks: Optional[str] = None

class StudentPerformance(Student):
    program: Optional[Program] = None
    assessments: List[StudentAssessmentResponse] = Field(default_factory=list)
    attendance_summary: List[AttendanceResponse] = Field(default_factory=list)
    # Backwards-compatible student dashboard payload (pivoted per subject)
    marks: List[ReportCardMark] = Field(default_factory=list)


class GradeDistributionItem(BaseModel):
    grade: str
    count: int


class SemesterPerformanceItem(BaseModel):
    semester: int
    subject_count: int
    average_internal: Optional[float] = 0.0
    average_grade_points: Optional[float] = 0.0
    backlog_count: int


class RiskSubjectItem(BaseModel):
    subject: str
    course_code: str
    semester: int
    grade: str
    internal_marks: Optional[float] = 0.0
    risk_reason: str


class StrengthSubjectItem(BaseModel):
    subject: str
    course_code: str
    semester: int
    grade: str
    score: Optional[float] = 0.0


class AttendanceInsight(BaseModel):
    total_present: int
    total_hours: int
    percentage: float
    recent_streak_days: int
    absent_days: int


class AnalyticsSummary(BaseModel):
    average_grade_points: float
    average_internal: float
    total_backlogs: int
    total_subjects: int
    grade_distribution: List[GradeDistributionItem] = Field(default_factory=list)
    semester_performance: List[SemesterPerformanceItem] = Field(default_factory=list)
    risk_subjects: List[RiskSubjectItem] = Field(default_factory=list)
    strength_subjects: List[StrengthSubjectItem] = Field(default_factory=list)
    attendance: AttendanceInsight


class StudentActionItem(BaseModel):
    model_config = ConfigDict(extra='forbid')

    title: str
    detail: str
    tone: str = Field(pattern='^(positive|warning|critical|info)$')


class StudentMetricCard(BaseModel):
    model_config = ConfigDict(extra='forbid')

    label: str
    value: float
    unit: Optional[str] = None
    trend: Optional[float] = None
    icon: Optional[str] = None
    hint: Optional[str] = None


class StudentRiskScore(BaseModel):
    model_config = ConfigDict(extra='forbid')

    roll_no: str
    name: str
    risk_score: Optional[float] = Field(default=0.0, ge=0, le=100)
    attendance_factor: Optional[float] = Field(default=0.0, ge=0, le=100)
    internal_marks_factor: Optional[float] = Field(default=0.0, ge=0, le=100)
    gpa_drop_factor: Optional[float] = Field(default=0.0, ge=0)
    is_at_risk: bool
    risk_level: str = Field(pattern='^(Critical|High|Moderate|Low)$')
    alerts: List[str]


class SemesterGradeRecord(BaseModel):
    semester: Optional[int] = None
    subject_code: Optional[str] = None
    subject_name: Optional[str] = None
    subject_title: Optional[str] = None
    credits: Optional[float] = None
    grade: Optional[str] = None
    marks: Optional[float] = None
    internal_marks: Optional[float] = None
    attempt: Optional[int] = None
    remarks: Optional[str] = None
    grade_point: Optional[float] = None


class StudentRecordHealth(BaseModel):
    completion_percentage: float
    available_sections: List[str] = Field(default_factory=list)
    missing_sections: List[str] = Field(default_factory=list)
    last_counselor_update: Optional[date] = None
    latest_activity_year: Optional[str] = None


class StudentCommandCenterResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')

    roll_no: str
    student_name: str
    batch: Optional[str] = None
    current_semester: Optional[int] = None
    program_name: Optional[str] = None
    class_rank: Optional[int] = None
    analytics: AnalyticsSummary
    risk: StudentRiskScore
    metrics: List[StudentMetricCard] = Field(default_factory=list)
    recommended_actions: List[StudentActionItem] = Field(default_factory=list)
    semester_focus: List[SemesterPerformanceItem] = Field(default_factory=list)
    recent_results: List[SemesterGradeRecord] = Field(default_factory=list)
    record_health: Optional[StudentRecordHealth] = None


class AdminStudentSnapshot(BaseModel):
    roll_no: str
    reg_no: Optional[str] = None
    name: str
    batch: Optional[str] = None
    section: Optional[str] = None
    program_name: Optional[str] = None
    current_semester: Optional[int] = None
    average_grade_points: float
    attendance_percentage: float
    backlogs: int
    is_initial_password: bool


class AdminOverview(BaseModel):
    total_students: int
    total_staff: int
    total_admins: int
    students_needing_attention: int
    average_attendance: Optional[float] = 0.0
    average_grade_points: Optional[float] = 0.0
    recent_students: List[AdminStudentSnapshot] = Field(default_factory=list)
    top_performers: List[AdminStudentSnapshot] = Field(default_factory=list)
    attention_required: List[AdminStudentSnapshot] = Field(default_factory=list)


class AdminDirectoryStudent(BaseModel):
    roll_no: str
    reg_no: Optional[str] = None
    name: str
    city: Optional[str] = None
    email: Optional[str] = None
    phone_primary: Optional[str] = None
    batch: Optional[str] = None
    current_semester: Optional[int] = None
    section: Optional[str] = None
    marks_count: int = 0
    attendance_count: int = 0
    attendance_percentage: Optional[float] = 0.0
    average_grade_points: Optional[float] = 0.0
    average_internal_percentage: Optional[float] = 0.0
    backlogs: int = 0
    rank: Optional[int] = None
    is_initial_password: bool = False


class AdminDirectoryInsightItem(BaseModel):
    label: str
    count: int


class AdminDirectoryInsights(BaseModel):
    total_records: int
    risk_students: int
    cities: List[AdminDirectoryInsightItem] = Field(default_factory=list)
    batches: List[AdminDirectoryInsightItem] = Field(default_factory=list)
    semesters: List[AdminDirectoryInsightItem] = Field(default_factory=list)
    missing_email_count: int = 0
    missing_phone_count: int = 0
    missing_batch_count: int = 0


class AdminRiskBreakdown(BaseModel):
    critical: int = 0
    warning: int = 0
    healthy: int = 0
    missing_data: int = 0


class AdminAnalyticsResponse(BaseModel):
    risk_breakdown: AdminRiskBreakdown
    batch_distribution: List[AdminDirectoryInsightItem] = Field(default_factory=list)
    semester_distribution: List[AdminDirectoryInsightItem] = Field(default_factory=list)
    city_distribution: List[AdminDirectoryInsightItem] = Field(default_factory=list)
    attendance_bands: List[AdminDirectoryInsightItem] = Field(default_factory=list)
    gpa_bands: List[AdminDirectoryInsightItem] = Field(default_factory=list)


class ContactInfoRecord(BaseModel):
    address: Optional[str] = None
    pincode: Optional[str] = None
    phone_primary: Optional[str] = None
    phone_secondary: Optional[str] = None
    phone_tertiary: Optional[str] = None
    email: Optional[str] = None
    city: Optional[str] = None


class FamilyDetailsRecord(BaseModel):
    father_name: Optional[str] = None
    mother_name: Optional[str] = None
    parent_occupation: Optional[str] = None
    parent_phone: Optional[str] = None
    parent_email: Optional[str] = None
    parent_address: Optional[str] = None
    emergency_contact_name: Optional[str] = None
    emergency_contact_relation: Optional[str] = None
    emergency_contact_phone: Optional[str] = None
    emergency_contact_email: Optional[str] = None
    emergency_contact_address: Optional[str] = None


class PreviousAcademicRecord(BaseModel):
    school_name: Optional[str] = None
    institution: Optional[str] = None
    board_university: Optional[str] = None
    passing_year: Optional[str] = None
    percentage: Optional[float] = None


class ExtraCurricularRecord(BaseModel):
    category: Optional[str] = None
    description: Optional[str] = None
    year: Optional[str] = None
    activity_type: Optional[str] = None


class CounselorDiaryRecord(BaseModel):
    semester: Optional[int] = None
    meeting_date: Optional[date] = None
    remark_category: Optional[str] = None
    remarks: Optional[str] = None
    action_planned: Optional[str] = None
    follow_up_date: Optional[date] = None
    counselor_name: Optional[str] = None
    counselor_id: Optional[int] = None
    created_at: Optional[datetime] = None




class InternalMarkRecord(BaseModel):
    semester: Optional[int] = None
    test_number: Optional[int] = None
    percentage: Optional[float] = None
    subject_code: Optional[str] = None
    subject_title: Optional[str] = None




class StudentAcademicSnapshot(BaseModel):
    semesters_tracked: int
    grade_entries: int
    internal_tests: int
    previous_qualifications: int
    cgpa_proxy: float
    best_grade: Optional[str] = None
    needs_attention: bool


class FullStudentRecord(BaseModel):
    roll_no: str
    core_profile: Optional[AdminDirectoryStudent] = None
    contact_info: Optional[ContactInfoRecord] = None
    family_details: Optional[FamilyDetailsRecord] = None
    previous_academics: List[PreviousAcademicRecord] = Field(default_factory=list)
    extra_curricular: List[ExtraCurricularRecord] = Field(default_factory=list)
    counselor_diary: List[CounselorDiaryRecord] = Field(default_factory=list)
    semester_grades: List[SemesterGradeRecord] = Field(default_factory=list)
    internal_marks: List[InternalMarkRecord] = Field(default_factory=list)
    record_health: Optional[StudentRecordHealth] = None
    academic_snapshot: Optional[StudentAcademicSnapshot] = None


class ScrapeInfo(BaseModel):
    Name: str
    RollNo: str
    RegNo: str
    Department: str
    Batch: str
    Semester: Optional[str] = None


class ScrapedMark(BaseModel):
    Sem: str
    Subject: str
    Grade: str


class AttendanceSummary(BaseModel):
    Semester: str
    Working: str
    Present: str
    Percentage: str


class DetailedAttendanceDay(BaseModel):
    Date: str
    HoursPerDay: str
    Status: List[str] = Field(default_factory=list)


class CitMarkEntry(BaseModel):
    SlNo: Optional[str] = None
    Date: Optional[str] = None
    Subject: Optional[str] = None
    Marks: Optional[str] = None
    Total: Optional[str] = None


class CitTestGroup(BaseModel):
    test_name: str
    entries: List[CitMarkEntry] = Field(default_factory=list)


class SemesterCitMarks(BaseModel):
    semester: str
    tests: List[CitTestGroup] = Field(default_factory=list)


class UniversityMark(BaseModel):
    SlNo: str
    Semester: str
    PaperCode: str
    PaperName: str
    Credit: str
    Grade: str
    GradePoint: str


class SyncMeta(BaseModel):
    attempts: int
    timeouts: List[int] = Field(default_factory=list)
    duration_seconds: float
    warnings: List[str] = Field(default_factory=list)
    used_cached_data: bool = False


class ScrapeResponse(BaseModel):
    status: str
    message: str
    info: Optional[ScrapeInfo] = None
    marks: List[ScrapedMark] = Field(default_factory=list)
    attendance_summary: List[AttendanceSummary] = Field(default_factory=list)
    detailed_attendance: List[DetailedAttendanceDay] = Field(default_factory=list)
    cit_marks: List[SemesterCitMarks] = Field(default_factory=list)
    university_marks: List[UniversityMark] = Field(default_factory=list)
    coe_results: List[dict] = Field(default_factory=list)
    meta: SyncMeta


class Token(BaseModel):
    access_token: str = Field(..., description="JWT access token")
    token_type: str = Field(..., description="Type of token, usually 'bearer'")
    refresh_token: str = Field(..., description="Refresh token to get a new access token")
    expires_in: int = Field(..., description="Token expiration in seconds")


class SubjectDifficultyItem(BaseModel):
    code: str
    subject: str
    semester: Optional[int] = None
    fail_rate: float
    average_grade_point: float
    average_internal: float
    variance: float
    difficulty_index: float
    pass_rate: float


class CohortAnalysisItem(BaseModel):
    batch: str
    students: int
    average_gpa: float
    average_attendance: float
    pass_rate: float
    average_risk: float
    readiness: float


class FacultyImpactItem(BaseModel):
    faculty: str
    subject: str
    pass_rate: float
    average_gpa: float
    impact_score: float
    note: Optional[str] = None


class AcademicInsight(BaseModel):
    type: str  # e.g., 'performance', 'risk', 'subject'
    message: str
    priority: str  # 'high', 'medium', 'low'


class HeatmapValue(BaseModel):
    subject: str
    score: float
    intensity: str  # 'strong', 'average', 'weak'


class HeatmapRow(BaseModel):
    roll_no: str
    name: str
    values: List[HeatmapValue]


class AdvancedAnalyticsDashboard(BaseModel):
    metrics: dict
    gpa_trend: List[dict]
    pass_fail_distribution: List[dict]
    subject_difficulty: List[SubjectDifficultyItem]
    cohorts: List[CohortAnalysisItem]
    faculty_impact: List[FacultyImpactItem]
    insights: List[str]
    heatmap: List[HeatmapRow]
    top_performers: List[dict]

class FacultyPerformance(BaseModel):
    faculty_id: str
    faculty_name: str
    grade_distribution: List[GradeDistributionItem]
    pass_percentage: float
    average_gpa: float

class SemesterTrendItem(BaseModel):
    semester: int
    average_gpa: float
    average_attendance: float
    student_count: int
    at_risk_count: int = 0

class DepartmentHealth(BaseModel):
    overall_health_score: float = Field(ge=0, le=100)
    active_students: int = Field(ge=0)
    at_risk_count: int = Field(ge=0)
    average_attendance: float = Field(ge=0, le=100)
    average_gpa: float = Field(ge=0, le=10)
    department_name: str = Field(min_length=2, max_length=80)
    daily_briefing: str = Field(min_length=1, max_length=400)
    semester_trends: List[SemesterTrendItem] = Field(default_factory=list)
    top_critical_subjects: List[SubjectDifficultyItem] = Field(default_factory=list)


class TrendPoint(BaseModel):
    semester: int = Field(ge=1, le=12)
    label: str
    average_gpa: float = Field(ge=0, le=10)
    average_attendance: float = Field(ge=1, le=100)
    student_count: int = Field(ge=0)
    at_risk_count: int = Field(ge=0)


class FailureHeatmapCell(BaseModel):
    model_config = ConfigDict(extra='forbid')

    subject_code: str
    subject_name: str
    semester: int = Field(ge=1, le=12)
    fail_rate: float = Field(ge=0, le=100)
    red_zone_count: int = Field(ge=0)
    attempts: int = Field(ge=0)


class FacultyImpactView(BaseModel):
    model_config = ConfigDict(extra='forbid')

    faculty_id: int = Field(ge=1)
    faculty_name: str
    subject_code: str
    subject_name: str
    average_gpa: float = Field(ge=0, le=10)
    pass_rate: float = Field(ge=0, le=100)
    impact_score: float = Field(ge=0, le=100)
    student_count: int = Field(ge=0)


class StudentStrengthRadar(BaseModel):
    model_config = ConfigDict(extra='forbid')

    roll_no: str
    name: str
    attendance: Optional[float] = Field(default=0.0, ge=0, le=100)
    internals: Optional[float] = Field(default=0.0, ge=0, le=100)
    gpa: Optional[float] = Field(default=0.0, ge=0, le=10)
    consistency: Optional[float] = Field(default=0.0, ge=0, le=100)


class DashboardMetric(BaseModel):
    model_config = ConfigDict(extra='forbid')

    value: float = Field(ge=0)
    change: Optional[float] = None
    label: str


class HODDashboardResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')

    department_health: DepartmentHealth
    metrics: dict[str, DashboardMetric]
    daily_briefing: str = Field(min_length=1, max_length=400)
    risk_students: List[StudentRiskScore] = Field(default_factory=list)
    trend_points: List[TrendPoint] = Field(default_factory=list)
    failure_heatmap: List[FailureHeatmapCell] = Field(default_factory=list)
    faculty_impact: List[FacultyImpactView] = Field(default_factory=list)
    strength_radar: List[StudentStrengthRadar] = Field(default_factory=list)
    directory: List[AdminDirectoryStudent] = Field(default_factory=list)


class PaginationMeta(BaseModel):
    model_config = ConfigDict(extra='forbid')

    total: int = Field(ge=0, description="Total number of items available")
    limit: int = Field(ge=1, description="Number of items per page")
    offset: int = Field(ge=0, description="Number of items skipped")


class AdminDirectoryPage(BaseModel):
    model_config = ConfigDict(extra='forbid')

    items: List[AdminDirectoryStudent] = Field(..., description="List of student records")
    pagination: PaginationMeta

    @property
    def __required__(self):
        return ["items", "pagination"]


class LeaderboardEntry(BaseModel):
    model_config = ConfigDict(extra='forbid')

    roll_no: str
    student_name: str
    section: Optional[str] = None
    batch: Optional[str] = None
    current_semester: Optional[int] = None
    subject_code: str
    subject_name: str
    total_marks: Optional[float] = Field(default=None, ge=0)
    internal_marks: Optional[float] = Field(default=None, ge=0)
    grade: Optional[str] = None
    class_rank: int = Field(ge=1)
    batch_rank: int = Field(ge=1)
    percentile: float = Field(ge=0, le=100)


class SubjectLeaderboardResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')

    subject_code: str = Field(..., description="Unique subject code")
    subject_name: str = Field(..., description="Subject name")
    top_leaderboard: List[LeaderboardEntry] = Field(..., description="List of top performers")
    bottom_leaderboard: List[LeaderboardEntry] = Field(..., description="List of low performers")
    pagination: PaginationMeta


class SubjectCatalogItem(BaseModel):
    model_config = ConfigDict(extra='forbid')

    id: int
    subject_code: str
    subject_name: str
    semester: Optional[int] = None
    records: int = Field(ge=0)
    is_active: bool = True


class SubjectToggleResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')
    
    id: int
    subject_code: str
    subject_name: str
    is_active: bool
    message: str


class StudentSemesterVelocity(BaseModel):
    model_config = ConfigDict(extra='forbid')

    semester: int = Field(ge=1)
    sgpa: float = Field(ge=0, le=10)
    previous_sgpa: Optional[float] = Field(default=None, ge=0, le=10)
    velocity: Optional[float] = None
    attendance_pct: float = Field(ge=0, le=100)
    internal_avg: float = Field(ge=0, le=100)


class StudentSkillDomainScore(BaseModel):
    model_config = ConfigDict(extra='forbid')

    domain: str
    score: float = Field(ge=0, le=100)
    cohort_score: Optional[float] = Field(default=None, ge=0, le=100)


class StudentSubjectHighlight(BaseModel):
    model_config = ConfigDict(extra='forbid')

    subject_code: str
    subject_name: str
    semester: int = Field(ge=1)
    grade: Optional[str] = None
    total_marks: Optional[float] = Field(default=None, ge=0)
    internal_marks: Optional[float] = Field(default=None, ge=0)
    score: Optional[float] = Field(default=None, ge=0)
    note: str


class StudentPeerBenchmark(BaseModel):
    model_config = ConfigDict(extra='forbid')

    cohort_size: int = Field(ge=1)
    class_rank: int = Field(ge=1)
    percentile: float = Field(ge=0, le=100)
    cohort_avg_gpa: float = Field(ge=0, le=10)
    gap_from_cohort: float


class StudentRiskDriver(BaseModel):
    model_config = ConfigDict(extra='forbid')

    label: str
    value: float
    status: str = Field(pattern='^(positive|warning|critical|neutral)$')


class Student360Profile(BaseModel):
    model_config = ConfigDict(extra='forbid')

    roll_no: str
    reg_no: Optional[str] = None
    student_name: str
    batch: Optional[str] = None
    section: Optional[str] = None
    current_semester: Optional[int] = None
    overall_gpa: float = Field(ge=0, le=10)
    attendance_percentage: float = Field(ge=0, le=100)
    gpa_trend: str = Field(pattern='^(Rising|Stable|Falling)$')
    gpa_velocity: float
    attendance_marks_correlation: Optional[float] = Field(default=None, ge=-1, le=1, description="Correlation between attendance and internal marks. Null when insufficient data points available.")
    active_arrears: int = Field(ge=0)
    risk_level: str = Field(pattern='^(Critical|High|Moderate|Low)$')
    attendance_band: str
    placement_signal: str
    skill_domains: List[StudentSkillDomainScore] = Field(default_factory=list)
    semester_velocity: List[StudentSemesterVelocity] = Field(default_factory=list)
    strongest_subjects: List[StudentSubjectHighlight] = Field(default_factory=list)
    support_subjects: List[StudentSubjectHighlight] = Field(default_factory=list)
    peer_benchmark: StudentPeerBenchmark
    risk_drivers: List[StudentRiskDriver] = Field(default_factory=list)
    recommended_actions: List[str] = Field(default_factory=list)


class SubjectBottleneckItem(BaseModel):
    model_config = ConfigDict(extra='forbid')

    subject_code: str
    subject_name: str
    semester: Optional[int] = None
    attempts: int = Field(ge=0)
    failure_rate: float = Field(ge=0, le=100)
    marks_stddev: float = Field(ge=0)
    current_average_marks: Optional[float] = Field(default=0.0, ge=0)
    historical_five_year_average: Optional[float] = Field(default=0.0, ge=0)
    drift_from_history: Optional[float] = Field(default=0.0)
    faculty_context: Optional[str] = None


class SubjectBottleneckResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')

    items: List[SubjectBottleneckItem] = Field(..., description="List of subject bottlenecks")
    pagination: PaginationMeta


class FacultyImpactMatrixItem(BaseModel):
    model_config = ConfigDict(extra='forbid')

    faculty_id: int = Field(ge=1)
    faculty_name: str
    subject_code: str
    subject_name: str
    student_count: int = Field(ge=0)
    failure_rate: float = Field(ge=0, le=100)
    subject_failure_rate: float = Field(ge=0, le=100)
    cohort_delta: float
    average_marks: float = Field(ge=0)
    impact_label: str


class FacultyImpactMatrixResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')

    items: List[FacultyImpactMatrixItem] = Field(..., description="List of faculty impact metrics")
    pagination: PaginationMeta


class PlacementCandidate(BaseModel):
    model_config = ConfigDict(extra='forbid')

    roll_no: str
    student_name: str
    batch: Optional[str] = None
    current_semester: Optional[int] = None
    cgpa: float = Field(ge=0, le=10)
    active_arrears: int = Field(ge=0)
    coding_subject_score: float = Field(ge=0, le=100)
    attendance_percentage: float = Field(ge=0, le=100)
    placement_ready: bool


class PlacementReadinessResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')

    items: List[PlacementCandidate] = Field(..., description="List of placement ready candidates")
    pagination: PaginationMeta


class SpotlightResult(BaseModel):
    model_config = ConfigDict(extra='forbid')

    entity_type: str = Field(pattern='^(student|faculty|subject)$')
    entity_id: str
    label: str
    sublabel: Optional[str] = None


class SpotlightSearchResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')

    results: List[SpotlightResult] = Field(default_factory=list)


class AdminRiskSummary(BaseModel):
    model_config = ConfigDict(extra='forbid')

    total: int = Field(ge=0)
    critical: int = Field(ge=0)
    high: int = Field(ge=0)
    moderate: int = Field(ge=0)
    low: int = Field(ge=0)


class AdminPlacementSummary(BaseModel):
    model_config = ConfigDict(extra='forbid')

    ready_count: int = Field(ge=0)
    almost_ready_count: int = Field(ge=0)
    blocked_count: int = Field(ge=0)
    avg_coding_score: float = Field(ge=0, le=100)


class StudentRankingRecord(BaseModel):
    """Individual student ranking record with CGPA-based rank"""
    model_config = ConfigDict(extra='forbid')
    
    roll_no: str
    reg_no: Optional[str] = None
    name: str
    batch: Optional[str] = None
    current_semester: Optional[int] = None
    section: Optional[str] = None
    cgpa: float = Field(ge=0, le=10, description="Cumulative GPA")
    attendance_percentage: float = Field(ge=0, le=100, description="Overall attendance percentage")
    backlogs: int = Field(ge=0, description="Number of active backlogs/arrears")
    rank: int = Field(ge=1, description="Overall rank based on CGPA")
    percentile: float = Field(ge=0, le=100, description="Percentile rank (0-100)")


class SemesterRankingRecord(BaseModel):
    """Individual student ranking record for semester-specific rankings with SGPA"""
    model_config = ConfigDict(extra='forbid')
    
    roll_no: str
    reg_no: Optional[str] = None
    name: str
    batch: Optional[str] = None
    current_semester: Optional[int] = None
    section: Optional[str] = None
    cgpa: float = Field(ge=0, le=10, description="Semester GPA (SGPA) for this specific semester")
    attendance_percentage: float = Field(ge=0, le=100, description="Overall attendance percentage")
    backlogs: int = Field(ge=0, description="Number of failed subjects in this semester")
    rank: int = Field(ge=1, description="Semester rank based on SGPA")
    percentile: float = Field(ge=0, le=100, description="Percentile rank (0-100)")
    avg_marks: float = Field(ge=0, le=200, description="Average marks in this semester")
    subjects_attempted: int = Field(ge=0, description="Number of subjects attempted in this semester")


class BatchRankingsResponse(BaseModel):
    """Response for batch-wise rankings"""
    model_config = ConfigDict(extra='forbid')
    
    batch: str
    total_students: int = Field(ge=0)
    rankings: List[StudentRankingRecord] = Field(default_factory=list)
    has_more: bool = Field(default=False, description="Whether more records are available")


class SemesterRankingsResponse(BaseModel):
    """Response for semester-wise rankings"""
    model_config = ConfigDict(extra='forbid')
    
    semester: int = Field(ge=1)
    total_students: int = Field(ge=0)
    rankings: List[SemesterRankingRecord] = Field(default_factory=list)
    has_more: bool = Field(default=False, description="Whether more records are available")


class SemesterBatchRankingsResponse(BaseModel):
    """Response for semester and batch specific rankings"""
    model_config = ConfigDict(extra='forbid')
    
    semester: int = Field(ge=1)
    batch: str
    total_students: int = Field(ge=0)
    rankings: List[SemesterRankingRecord] = Field(default_factory=list)
    has_more: bool = Field(default=False, description="Whether more records are available")


class OverallRankingsResponse(BaseModel):
    """Response for overall rankings across all students"""
    model_config = ConfigDict(extra='forbid')
    
    total_students: int = Field(ge=0)
    rankings: List[StudentRankingRecord] = Field(default_factory=list)
    has_more: bool = Field(default=False, description="Whether more records are available")


class StudentRankDetails(BaseModel):
    """Detailed ranking information for a specific student"""
    model_config = ConfigDict(extra='forbid')
    
    roll_no: str
    name: str
    rank: int = Field(ge=1, description="Current rank")
    cgpa: float = Field(ge=0, le=10, description="Current CGPA")
    attendance_percentage: float = Field(ge=0, le=100, description="Attendance percentage")
    percentile: float = Field(ge=0, le=100, description="Percentile rank")
    total_students: int = Field(ge=0, description="Total students in comparison")
    backlogs: int = Field(ge=0, description="Number of active backlogs")
    rank_category: str = Field(description="Performance category: Excellent, Good, Average, or Needs Improvement")


class AdminLeaderboardSnapshot(BaseModel):
    model_config = ConfigDict(extra='forbid')

    subject_code: str
    subject_name: str
    semester: Optional[int] = None
    attempts: int = Field(ge=0)
    top_score: float = Field(ge=0)
    median_score: float = Field(ge=0)
    score_spread: float = Field(ge=0)


class AdminSubjectCoverage(BaseModel):
    model_config = ConfigDict(extra='forbid')

    semester: int = Field(ge=1)
    total_subjects: int = Field(ge=0)
    ranked_subjects: int = Field(ge=0)
    total_records: int = Field(ge=0)


class AdminCohortAction(BaseModel):
    model_config = ConfigDict(extra='forbid')

    title: str
    detail: str
    metric: str
    tone: str = Field(pattern='^(positive|warning|critical|info)$')


class BatchHealthItem(BaseModel):
    batch: str
    average_gpa: float
    average_attendance: float
    at_risk_count: int
    total_students: int

class AdminCommandCenterResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')

    daily_briefing: str
    department_health: DepartmentHealth
    alerts: List[str] = Field(default_factory=list)
    bottlenecks: List[SubjectBottleneckItem] = Field(default_factory=list)
    faculty_impact: List[FacultyImpactMatrixItem] = Field(default_factory=list)
    placement_ready: List[PlacementCandidate] = Field(default_factory=list)
    spotlight_results: List[SpotlightResult] = Field(default_factory=list)
    top_performers: List[AdminDirectoryStudent] = Field(default_factory=list)
    attendance_defaulters: List[AdminDirectoryStudent] = Field(default_factory=list)
    internal_defaulters: List[AdminDirectoryStudent] = Field(default_factory=list)
    backlog_clusters: List[AdminDirectoryStudent] = Field(default_factory=list)
    opportunity_students: List[AdminDirectoryStudent] = Field(default_factory=list)
    watchlist_students: List[StudentRiskScore] = Field(default_factory=list)
    batch_health: List[BatchHealthItem] = Field(default_factory=list)
    semester_pulse: List[SemesterTrendItem] = Field(default_factory=list)
    risk_summary: AdminRiskSummary
    placement_summary: AdminPlacementSummary
    leaderboard_snapshots: List[AdminLeaderboardSnapshot] = Field(default_factory=list)
    subject_coverage: List[AdminSubjectCoverage] = Field(default_factory=list)
    action_queue: List[AdminCohortAction] = Field(default_factory=list)
    quick_actions: List[str] = Field(default_factory=list)
    subject_catalog: List[SubjectCatalogItem] = Field(default_factory=list)


class RiskRegistryResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')

    items: List[StudentRiskScore] = Field(..., description="List of at-risk students")
    pagination: PaginationMeta


# Rebuild models to handle nested structures and forward references
StudentPerformance.model_rebuild()
DepartmentHealth.model_rebuild()
HODDashboardResponse.model_rebuild()
AdminCommandCenterResponse.model_rebuild()
RiskRegistryResponse.model_rebuild()

class StaffSubject(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    subject_id: int
    subject_name: str
    course_code: str
    semester: int
    section: Optional[str] = None
    academic_year: Optional[str] = None
    student_count: int = 0
    average_marks: Optional[float] = 0.0
    pass_percentage: Optional[float] = 0.0
    average_attendance: Optional[float] = 0.0

class StaffStudentMarkRow(BaseModel):
    student_id: int
    roll_no: str
    name: str
    cit1: Optional[float] = None
    cit2: Optional[float] = None
    cit3: Optional[float] = None
    semester_exam: Optional[float] = None

class StaffStudentAssessmentUpdate(BaseModel):
    student_id: int
    subject_id: int
    semester: int
    assessment_type: str
    marks: Optional[float] = None
    remarks: Optional[str] = None
    attempt: Optional[int] = 1

class RecentMarkUpdate(BaseModel):
    subject_name: str
    student_name: str
    roll_no: str
    action: str
    updated_at: datetime

class StaffDashboardResponse(BaseModel):
    staff_id: int
    name: str
    department: Optional[str] = None
    subjects: List[StaffSubject] = Field(default_factory=list)
    total_students_handled: int = 0
    recent_marks_updates: List[RecentMarkUpdate] = Field(default_factory=list)
    average_performance: float = 0.0
    pending_marks_count: int = 0

class StaffTimeTableEntry(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    day_of_week: int
    period: int  # Changed from hour to period to match TimeTable model
    subject_id: int
    subject_name: str
    course_code: str
    section: Optional[str] = None
    semester: Optional[int] = None


class StaffProfile(BaseModel):
    id: int
    username: str
    name: str
    email: Optional[EmailStr] = None
    department: Optional[str] = None
    created_at: Optional[datetime] = None
    subjects: Optional[List[dict]] = Field(default_factory=list, description="Assigned subjects with code and name")


class StaffSubjectAssign(BaseModel):
    subject_ids: List[int] = Field(default_factory=list, description="Subject IDs to assign to this staff member")
    subject_codes: List[str] = Field(default_factory=list, description="Subject codes to assign (alternative to IDs)")


class StaffCreate(BaseModel):
    username: str = Field(min_length=3, max_length=50, pattern=r'^[a-zA-Z0-9_.-]+$')
    password: str = Field(min_length=6, description="Password must be at least 6 characters")
    name: str = Field(min_length=2, max_length=255)
    email: Optional[EmailStr] = None
    department: Optional[str] = Field(None, max_length=100)


class StaffUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=2, max_length=255)
    email: Optional[EmailStr] = None
    department: Optional[str] = Field(None, max_length=100)
    password: Optional[str] = Field(None, min_length=6, description="Password must be at least 6 characters")

class AttendanceMarkRequest(BaseModel):
    subject_id: int
    date: str # YYYY-MM-DD
    period: int  # Changed from hour to period to match database model
    absentees: List[str] = Field(default_factory=list) # Roll numbers
    section: str
    semester: int


# Timetable Schemas
class TimetableBase(BaseModel):
    batch: str = Field(..., min_length=1, max_length=20)
    section: str = Field(..., pattern=r'^[A-D]$')
    day_of_week: int = Field(..., ge=1, le=7)  # 1=Monday, 7=Sunday
    period: int = Field(..., ge=1, le=8)  # 1-8 periods per day
    subject_id: Optional[int] = None
    faculty_id: Optional[int] = None
    semester: Optional[int] = Field(None, ge=1, le=8)
    academic_year: Optional[str] = Field(None, max_length=20)


class TimetableCreate(TimetableBase):
    pass


class TimetableUpdate(BaseModel):
    subject_id: Optional[int] = None
    faculty_id: Optional[int] = None
    semester: Optional[int] = Field(None, ge=1, le=8)
    academic_year: Optional[str] = Field(None, max_length=20)


class TimetableResponse(TimetableBase):
    id: int
    created_at: datetime
    updated_at: datetime
    
    # Optional nested objects
    subject_name: Optional[str] = None
    subject_code: Optional[str] = None
    faculty_name: Optional[str] = None
    
    class Config:
        from_attributes = True


class TimetableSlot(BaseModel):
    """A single slot in the timetable grid"""
    period: int
    subject_name: Optional[str] = None
    subject_code: Optional[str] = None
    faculty_name: Optional[str] = None
    timetable_id: Optional[int] = None  # For editing


class TimetableDay(BaseModel):
    """One day's schedule"""
    day_of_week: int
    day_name: str
    slots: List[TimetableSlot]


class TimetableWeeklyView(BaseModel):
    """Complete weekly timetable for a batch/section"""
    batch: str
    section: str
    academic_year: Optional[str] = None
    semester: Optional[int] = None
    days: List[TimetableDay]


class TimetableListResponse(BaseModel):
    items: List[TimetableResponse]
    total: int
    batch: Optional[str] = None
    section: Optional[str] = None
