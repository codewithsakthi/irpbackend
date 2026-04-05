import importlib.util
import json
import re
import time
from datetime import datetime
from pathlib import Path

from sqlalchemy import text, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from ..core import auth
from .. import models

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = PROJECT_ROOT / 'pipeline' / 'script.py'
DATA_DIR = PROJECT_ROOT / 'data'


def _load_script_scraper():
    if not SCRIPT_PATH.exists():
        return None
    try:
        spec = importlib.util.spec_from_file_location('automation_script_scraper', SCRIPT_PATH)
        if not spec or not spec.loader:
            return None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return getattr(module, 'get_parent_portal_info', None)
    except Exception:
        return None


script_get_parent_portal_info = _load_script_scraper()


class PortalScraper:
    # Official R2024 MCA Curriculum Mapping
    MCA_R2024_CURRICULUM = {
        # Semester 1
        "PROBABILITY AND STATISTICS": {"code": "24FC101", "sem": 1, "credits": 4},
        "ADVANCED DATABASE TECHNOLOGY": {"code": "24MC102", "sem": 1, "credits": 3},
        "PYTHON PROGRAMMING": {"code": "24MC103", "sem": 1, "credits": 3},
        "OBJECT ORIENTED SOFTWARE ENGINEERING": {"code": "24MC104", "sem": 1, "credits": 3},
        "MODERN OPERATING SYSTEMS": {"code": "24MC105", "sem": 1, "credits": 3},
        "RESEARCH METHODOLOGY AND IPR": {"code": "24RM101", "sem": 1, "credits": 3},
        "PYTHON PROGRAMMING LABORATORY": {"code": "24MC1L1", "sem": 1, "credits": 1.5},
        "ADVANCED DATABASE TECHNOLOGY LABORATORY": {"code": "24MC1L2", "sem": 1, "credits": 1.5},
        "COMMUNICATION SKILLS LABORATORY - I": {"code": "24MC1L3", "sem": 1, "credits": 1},
        "STRESS MANAGEMENT BY YOGA": {"code": "24AC107", "sem": 1, "credits": 0},
        
        # Semester 2
        "INTERNET OF THINGS": {"code": "24MC201", "sem": 2, "credits": 3},
        "DATA STRUCTURES AND ALGORITHMS": {"code": "24MC202", "sem": 2, "credits": 4},
        "MACHINE LEARNING": {"code": "24MC203", "sem": 2, "credits": 3},
        "ADVANCED JAVA": {"code": "24MC204", "sem": 2, "credits": 4},
        "MOBILE COMPUTING": {"code": "24MC2E2", "sem": 2, "credits": 3},
        "OPERATION RESEARCH": {"code": "24MC2E6", "sem": 2, "credits": 3},
        "OPERATIONS RESEARCH": {"code": "24MC2E6", "sem": 2, "credits": 3},
        "DATA STRUCTURES AND ALGORITHMS LABORATORY": {"code": "24MC2L1", "sem": 2, "credits": 2},
        "ADVANCED JAVA LABORATORY": {"code": "24MC2L2", "sem": 2, "credits": 2},
        "MACHINE LEARNING LABORATORY": {"code": "24MC2L3", "sem": 2, "credits": 2},
        "COMMUNICATION SKILLS LABORATORY - II": {"code": "24MC2L4", "sem": 2, "credits": 1},
    }

    def __init__(self):
        self.snapshot_dir = DATA_DIR

    def _parse_dob(self, dob: str):
        for fmt in ('%d%m%Y', '%d/%m/%Y', '%Y-%m-%d'):
            try:
                return datetime.strptime(dob, fmt).date()
            except ValueError:
                continue
        return None

    def _normalize_dob_password(self, dob: str):
        parsed = self._parse_dob(dob)
        if parsed:
            return parsed.strftime('%d%m%Y')
        digits = re.sub(r'\D', '', dob or '')
        return digits[:8] if digits else None

    async def _get_student_role(self, db: AsyncSession):
        result = await db.execute(select(models.Role).filter(models.Role.name == 'student'))
        return result.scalars().first()

    def _normalize_grade(self, grade: str | None):
        if not grade:
            return None
        normalized = str(grade).strip().upper()
        grade_map = {
            'PASS': 'P',
            'FAIL': 'F',
            'ABSENT': 'AB',
        }
        normalized = grade_map.get(normalized, normalized)
        return normalized[:2]

    def _load_snapshot(self, roll_no: str):
        path = self.snapshot_dir / f'{roll_no}_data.json'
        if not path.exists():
            return None
        try:
            with path.open('r', encoding='utf-8') as handle:
                payload = json.load(handle)
        except Exception:
            return None

        parent = payload.get('ParentPortal', {})
        info = parent.get('Info') or {}
        marks = parent.get('Marks') or []
        detailed_attendance = parent.get('DetailedAttendance') or {}
        attendance_summary = parent.get('AttendanceSummary') or []
        cit_marks = parent.get('CITMarks') or {}
        university_marks = parent.get('UniversityMarks') or []
        coe_results = payload.get('COEResults') or []
        if not info:
            return None
        return info, marks, detailed_attendance, attendance_summary, cit_marks, university_marks, coe_results

    async def _get_or_create_subject(self, subject_desc, semester, db: AsyncSession):
        course_code = subject_desc.split('-', 1)[0].strip() if '-' in subject_desc else subject_desc.strip()[:20]
        result = await db.execute(select(models.Subject).filter(models.Subject.course_code == course_code))
        subject = result.scalars().first()
        if not subject:
            name = subject_desc.split('-', 1)[1].strip() if '-' in subject_desc else subject_desc.strip()
            subject = models.Subject(course_code=course_code, name=name, semester=semester)
            db.add(subject)
            await db.flush()
        return subject

    async def _find_subject_by_name(self, subject_name, db: AsyncSession, semester=None):
        normalized = re.sub(r'\s+', ' ', subject_name).strip().upper()
        
        # 1. Exact Match in DB
        result = await db.execute(select(models.Subject))
        subjects = result.scalars().all()
        for s in subjects:
            if s.name and normalized == re.sub(r'\s+', ' ', s.name).strip().upper():
                return s
        
        # 2. Curriculum Mapping check
        if normalized in self.MCA_R2024_CURRICULUM:
            info = self.MCA_R2024_CURRICULUM[normalized]
            result = await db.execute(select(models.Subject).filter(models.Subject.course_code == info['code']))
            subject = result.scalars().first()
            if not subject:
                subject = models.Subject(
                    course_code=info['code'],
                    name=normalized,
                    credits=info['credits'],
                    semester=info['sem'] or semester
                )
                db.add(subject)
                await db.flush()
            return subject

        # 3. Partial Match in DB
        for s in subjects:
            if s.name and normalized in re.sub(r'\s+', ' ', s.name).strip().upper():
                return s
        return None

    async def _sync_student_record(self, roll_no, dob, info, db: AsyncSession):
        result = await db.execute(select(models.Student).filter(models.Student.roll_no == roll_no))
        student = result.scalars().first()
        result = await db.execute(select(models.User).options(joinedload(models.User.role)).filter(models.User.username == roll_no))
        user = result.scalars().first()
        
        # Safeguard: If the existing user is an admin/staff, we MUST NOT link the student profile to them.
        # We'll create a new student-specific user instead if the IDs don't match correctly.
        if user and user.role and user.role.name in ['admin', 'staff']:
            print(f"Warning: Found existing {user.role.name} user with username {roll_no}. Creating separate student account.")
            # We'll use a prefix or different strategy if roll_no conflicts with admin/staff usernames
            result = await db.execute(select(models.User).filter(models.User.username == f"stu_{roll_no}"))
            user = result.scalars().first()

        if not user:
            student_role = await self._get_student_role(db)
            if not student_role:
                raise ValueError('Student role not found')
            initial_password = self._normalize_dob_password(dob)
            if not initial_password:
                raise ValueError(f'Unable to derive initial password from DOB for {roll_no}')
            
            # Use original roll_no unless it conflicts with a non-student user
            target_username = roll_no
            result = await db.execute(select(models.User).filter(models.User.username == target_username))
            potential_conflict = result.scalars().first()
            if potential_conflict:
                target_username = f"stu_{roll_no}"

            user = models.User(
                username=target_username,
                password_hash=auth.get_password_hash(initial_password),
                role_id=student_role.id,
                is_initial_password=True,
            )
            try:
                db.add(user)
                await db.flush()
            except IntegrityError:
                await db.rollback()
                result = await db.execute(select(models.User).filter(models.User.username == target_username))
                user = result.scalars().first()
                if not user:
                    raise

        if not student:
            student = models.Student(
                id=user.id,
                roll_no=roll_no,
                name=info.get('Name', roll_no),
                reg_no=info.get('RegNo'),
                batch=info.get('Batch'),
                email=info.get('Email') or None,
                dob=self._parse_dob(dob) or datetime.utcnow().date(),
            )
            db.add(student)
            await db.flush()

        if student:
            student.name = info.get('Name', student.name)
            student.reg_no = info.get('RegNo') or student.reg_no
            student.batch = info.get('Batch') or student.batch
            student.email = info.get('Email') or student.email
            parsed_dob = self._parse_dob(dob)
            if parsed_dob:
                student.dob = parsed_dob
            semester = str(info.get('Semester', '')).strip()
            if semester.isdigit():
                student.current_semester = int(semester)
            await db.flush()
        return student

    async def _sync_marks_to_db(self, student, marks, cit_marks, university_marks, db: AsyncSession):
        if not student:
            return

        # 2. Process University Marks (Semester Exam Grades)
        print(f"DEBUG: Processing {len(university_marks)} university marks for {student.roll_no}")
        for item in university_marks:
            semester = int(item.get('Semester', 0))
            subject_code = item.get('PaperCode', '').strip()
            subject_name = item.get('PaperName', '').strip()
            grade = item.get('Grade', '').strip()
            grade_point = item.get('GradePoint', '0')
            
            if not subject_code:
                continue

            # ORM Update
            result = await db.execute(select(models.Subject).filter(models.Subject.course_code == subject_code))
            subject = result.scalars().first()
            if not subject:
                credit = item.get('Credit', '0')
                subject = models.Subject(
                    course_code=subject_code,
                    name=subject_name,
                    credits=int(float(credit)) if str(credit).replace('.', '', 1).isdigit() else 0,
                    semester=semester,
                )
                db.add(subject)
                await db.flush()
            
            result = await db.execute(select(models.StudentAssessment).filter(
                models.StudentAssessment.student_id == student.id,
                models.StudentAssessment.subject_id == subject.id,
                models.StudentAssessment.semester == semester,
                models.StudentAssessment.assessment_type == 'SEMESTER_EXAM'
            ))
            entry = result.scalars().first()
            if not entry:
                entry = models.StudentAssessment(
                    student_id=student.id, 
                    subject_id=subject.id, 
                    semester=semester,
                    assessment_type='SEMESTER_EXAM'
                )
                db.add(entry)
            
            # Map grade_point to marks (GradePoint is usually 0-10, so * 10)
            try:
                mark_val = float(grade_point) * 10
                entry.marks = mark_val
            except (ValueError, TypeError):
                entry.marks = 0.0

            if grade:
                entry.remarks = f"Grade: {grade}"
            await db.flush()

        # 3. Process CIT Marks (Internal Percentage)
        test_field_map = {'Test_1': 'cit1_marks', 'Test_2': 'cit2_marks', 'Test_3': 'cit3_marks'}
        for semester_key, tests in cit_marks.items():
            semester_match = re.search(r'(\d+)$', semester_key)
            semester = int(semester_match.group(1)) if semester_match else 0
            for test_name, entries in tests.items():
                test_num = int(test_name.split('_')[1]) if '_' in test_name else 1
                for item in entries:
                    if 'Subject' not in item or 'Marks' not in item:
                        continue
                    subject = await self._find_subject_by_name(item['Subject'], db, semester=semester)
                    if not subject:
                        # Fallback for truly unknown subjects
                        clean_name = re.sub(r'[^A-Z\s]', '', item['Subject'].upper()).strip()
                        code = f"CIT_{clean_name[:10].replace(' ', '_')}"
                        subject = models.Subject(course_code=code, name=item['Subject'], credits=0, semester=semester)
                        db.add(subject)
                        await db.flush()

                    raw_val = item.get('Marks')
                    if raw_val is None or str(raw_val).strip() == '':
                        continue
                        
                    try:
                        if str(raw_val).strip() == "-- A --":
                            mark_val = -1.0
                        else:
                            mark_val = round(float(raw_val), 2)
                    except (ValueError, TypeError):
                        continue
                    
                    # Find existing CIT assessment
                    result = await db.execute(select(models.StudentAssessment).filter(
                        models.StudentAssessment.student_id == student.id,
                        models.StudentAssessment.subject_id == subject.id,
                        models.StudentAssessment.semester == semester,
                        models.StudentAssessment.assessment_type == f"CIT{test_num}"
                    ))
                    entry = result.scalars().first()
                    if not entry:
                        entry = models.StudentAssessment(
                            student_id=student.id,
                            subject_id=subject.id,
                            semester=semester,
                            assessment_type=f"CIT{test_num}"
                        )
                        db.add(entry)
                    
                    if mark_val is not None:
                        entry.marks = mark_val
                        await db.flush()

    async def _sync_attendance_to_db(self, student, detailed_attendance, db: AsyncSession):
        if not student:
            return
            
        for sem_key, days in detailed_attendance.items():
            semester_match = re.search(r'(\d+)$', sem_key)
            semester = int(semester_match.group(1)) if semester_match else 1
            
            for day in days:
                try:
                    att_date = datetime.strptime(day['Date'], '%d-%m-%Y').date()
                except ValueError:
                    continue
                
                status_array = day.get('Status', [])
                for idx, status in enumerate(status_array):
                    period = idx + 1
                    norm_status = 'P' if status.upper() in {'P', 'OD'} else 'A'
                    
                    # Upsert into PeriodAttendance
                    result = await db.execute(select(models.PeriodAttendance).filter(
                        models.PeriodAttendance.student_id == student.id,
                        models.PeriodAttendance.date == att_date,
                        models.PeriodAttendance.period == period
                    ))
                    existing = result.scalars().first()
                    if existing:
                        existing.status = norm_status
                    else:
                        db.add(models.PeriodAttendance(
                            student_id=student.id,
                            date=att_date,
                            period=period,
                            status=norm_status,
                            semester=semester
                        ))
                await db.flush()

    def _flatten_cit_marks(self, cit_marks):
        return [
            {
                'semester': semester,
                'tests': [{'test_name': test_name, 'entries': entries} for test_name, entries in tests.items()]
            }
            for semester, tests in cit_marks.items()
        ]

    def _flatten_detailed_attendance(self, detailed_attendance):
        flattened = []
        for _, days in detailed_attendance.items():
            flattened.extend(days)
        return flattened

    def _build_response(self, status, message, info, marks, detailed_attendance, attendance_summary, cit_marks, university_marks, coe_results, started_at, warnings, used_cached_data):
        return {
            'status': status,
            'message': message,
            'info': info,
            'marks': marks,
            'attendance_summary': attendance_summary,
            'detailed_attendance': self._flatten_detailed_attendance(detailed_attendance),
            'cit_marks': self._flatten_cit_marks(cit_marks),
            'university_marks': university_marks,
            'coe_results': coe_results,
            'meta': {
                'attempts': 1,
                'timeouts': [],
                'duration_seconds': round(time.time() - started_at, 2),
                'warnings': warnings,
                'used_cached_data': used_cached_data,
            },
        }

    async def sync_payload_to_db(self, roll_no: str, dob: str, payload: dict, db: AsyncSession):
        """
        Generic method to sync a student data payload (from script.py JSON format) to the database.
        """
        student_info = payload.get('StudentInfo', {}) or {}
        parent = payload.get('ParentPortal', {})
        info = parent.get('Info') or {}
        if student_info:
            info = {
                **info,
                'Name': info.get('Name') or student_info.get('Name'),
                'RollNo': info.get('RollNo') or student_info.get('Roll No') or roll_no,
                'RegNo': info.get('RegNo') or student_info.get('RegNo'),
                'Email': student_info.get('Email address'),
            }
        marks = parent.get('Marks') or []
        detailed_attendance = parent.get('DetailedAttendance') or {}
        attendance_summary = parent.get('AttendanceSummary') or []
        cit_marks = parent.get('CITMarks') or {}
        university_marks = parent.get('UniversityMarks') or []

        if not info:
            return None

        student = await self._sync_student_record(roll_no, dob, info, db)
        if student:
            await self._sync_marks_to_db(student, marks, cit_marks, university_marks, db)
            await self._sync_attendance_to_db(student, detailed_attendance, db)
            await db.commit()
            await db.refresh(student)
        return student

    async def import_snapshot_file(self, file_path: Path, db: AsyncSession):
        with file_path.open('r', encoding='utf-8') as handle:
            payload = json.load(handle)

        student_info = payload.get('StudentInfo', {}) or {}
        roll_no = student_info.get('Roll No') or file_path.name.replace('_data.json', '')
        dob = student_info.get('Date Of Birth')
        if not dob:
            raise ValueError(f'DOB not found in {file_path.name}')

        student = await self.sync_payload_to_db(roll_no, dob, payload, db)
        if not student:
            raise ValueError(f'Invalid payload in {file_path.name}')

        return {
            'roll_no': roll_no,
            'name': student.name,
            'username': roll_no,
            'initial_password': self._normalize_dob_password(dob),
            'file_name': file_path.name,
        }

    async def import_all_snapshots(self, db: AsyncSession):
        if not self.snapshot_dir.exists():
            raise FileNotFoundError(f'Data directory not found: {self.snapshot_dir}')

        imported = []
        errors = []
        for file_path in sorted(self.snapshot_dir.glob('*_data.json')):
            try:
                imported.append(await self.import_snapshot_file(file_path, db))
            except Exception as exc:
                await db.rollback()
                errors.append({'file_name': file_path.name, 'error': str(exc)})

        return {
            'imported_count': len(imported),
            'error_count': len(errors),
            'imported_students': imported,
            'errors': errors,
        }

    async def get_parent_portal_data(self, roll_no: str, dob: str, db: AsyncSession):
        started_at = time.time()
        warnings = []

        live_payload = None
        if script_get_parent_portal_info:
            try:
                live_payload = script_get_parent_portal_info(roll_no, dob)
            except Exception as exc:
                warnings.append(f'Live portal scrape failed: {exc}')
        else:
            warnings.append(f'Unable to load script scraper from {SCRIPT_PATH}')

        if live_payload and live_payload[0]:
            if len(live_payload) == 7:
                info, marks, detailed_attendance, attendance_summary, cit_marks, university_marks, coe_results = live_payload
            else:
                info, marks, detailed_attendance, attendance_summary, cit_marks, university_marks = live_payload
                coe_results = []
            
            # Note: live_payload matches the tuple structure returned by script.get_parent_portal_info
            student = await self._sync_student_record(roll_no, dob, info, db)
            await self._sync_marks_to_db(student, marks, cit_marks, university_marks, db)
            # Optionally handle coe_results here if there's a model for it. 
            # For now, coe_results might be redundant with university_marks if they overlap, 
            # but we can store them if needed.
            await self._sync_attendance_to_db(student, detailed_attendance, db)
            
            # Since build_response needs coe_results (which we should add to its signature)
            return self._build_response(
                'success',
                'Portal data synced successfully.',
                info,
                marks,
                detailed_attendance,
                attendance_summary,
                cit_marks,
                university_marks,
                coe_results,
                started_at,
                warnings,
                False,
            )

        # Fallback to SNAPSHOT file (stored in data/ folder)
        snapshot_payload = self._load_snapshot(roll_no)
        if snapshot_payload:
            warnings.append(f'Live portal did not return student data. Loaded snapshot from {self.snapshot_dir}.')
            info, marks, detailed_attendance, attendance_summary, cit_marks, university_marks, coe_results = snapshot_payload
            student = await self._sync_student_record(roll_no, dob, info, db)
            await self._sync_marks_to_db(student, marks, cit_marks, university_marks, db)
            await self._sync_attendance_to_db(student, detailed_attendance, db)
            return self._build_response(
                'cached',
                'Live portal is unavailable right now, so the latest saved snapshot was loaded.',
                info,
                marks,
                detailed_attendance,
                attendance_summary,
                cit_marks,
                university_marks,
                coe_results,
                started_at,
                warnings,
                True,
            )

        return {
            'status': 'failed',
            'message': 'The parent portal did not return student data, and no saved snapshot was available.',
            'info': None,
            'marks': [],
            'attendance_summary': [],
            'detailed_attendance': [],
            'cit_marks': [],
            'university_marks': [],
            'meta': {
                'attempts': 1,
                'timeouts': [],
                'duration_seconds': round(time.time() - started_at, 2),
                'warnings': warnings,
                'used_cached_data': False,
            },
        }
