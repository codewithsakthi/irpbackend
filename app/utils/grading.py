from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional


@dataclass(frozen=True)
class ComputedGrade:
    internal: Optional[float]
    exam: Optional[float]
    total: Optional[float]
    grade: Optional[str]
    result_status: Optional[str]
    grade_point: Optional[float]


def _to_float(value) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except Exception:
        return None


def internal_best2_avg(cit1, cit2, cit3) -> Optional[float]:
    """Average of best 2 out of 3 CIT marks.

    Rules:
    - 3 marks: drop the lowest, average remaining 2
    - 2 marks: average them
    - 1 mark: use it as-is
    - none: None
    """
    values = [_to_float(cit1), _to_float(cit2), _to_float(cit3)]
    values = [v for v in values if v is not None]
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    if len(values) == 2:
        return (values[0] + values[1]) / 2.0
    values.sort(reverse=True)
    return (values[0] + values[1]) / 2.0


def total_marks(internal: Optional[float], exam: Optional[float]) -> Optional[float]:
    if internal is None and exam is None:
        return None
    return float((internal or 0) + (exam or 0))


def grade_from_total(total: Optional[float]) -> Optional[str]:
    if total is None:
        return None
    if total >= 90:
        return "O"
    if total >= 80:
        return "A+"
    if total >= 70:
        return "A"
    if total >= 60:
        return "B+"
    if total >= 50:
        return "B"
    if total >= 45:
        return "C"
    return "F"


def grade_point_from_grade(grade: Optional[str]) -> Optional[float]:
    if not grade:
        return None
    grade = str(grade).strip().upper()
    mapping = {
        "O": 10.0,
        "A+": 9.0,
        "A": 8.0,
        "B+": 7.0,
        "B": 6.0,
        "C": 5.0,
        "F": 0.0,
        "P": 5.0,  # audit/pass marker
        "PASS": 5.0,
    }
    return mapping.get(grade)


def result_status_from_total(total: Optional[float]) -> Optional[str]:
    if total is None:
        return None
    return "PASS" if total >= 50 else "FAIL"


def compute_grade(
    *,
    course_code: Optional[str],
    cit1=None,
    cit2=None,
    cit3=None,
    semester_exam=None,
    lab=None,
    project=None,
) -> ComputedGrade:
    code = (course_code or "").upper()
    is_audit = code.startswith("24AC")

    c1 = _to_float(cit1)
    c2 = _to_float(cit2)
    c3 = _to_float(cit3)
    exam_component = _to_float(semester_exam)
    if exam_component is None:
        exam_component = _to_float(lab) if _to_float(lab) is not None else _to_float(project)

    # Remove automatic P grade assignment for audit courses
    # All courses (including audit) now only get grades when assessments are taken
    
    internal = internal_best2_avg(c1, c2, c3)
    total = total_marks(internal, exam_component)
    grade = grade_from_total(total)
    return ComputedGrade(
        internal=internal,
        exam=exam_component,
        total=total,
        grade=grade,
        result_status=result_status_from_total(total),
        grade_point=grade_point_from_grade(grade),
    )

