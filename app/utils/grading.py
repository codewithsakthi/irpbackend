from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, List, Dict, Tuple


@dataclass(frozen=True)
class ComputedGrade:
    internal: Optional[float]
    exam: Optional[float]
    total: Optional[float]
    grade: Optional[str]
    result_status: Optional[str]
    grade_point: Optional[float]


@dataclass(frozen=True)
class HybridPerformanceMetrics:
    """Enhanced performance metrics including percentile and hybrid classification"""
    percentile: Optional[float]
    normalized_score: Optional[float]
    performance_label: Optional[str]
    subject_average: Optional[float]
    
    
@dataclass(frozen=True)
class SubjectPerformanceData:
    """Data structure for subject-wise performance calculations"""
    subject_id: int
    student_marks: List[Tuple[int, float]]  # (student_id, marks)
    pass_threshold: float = 50.0
    percentile_excellent: float = 85.0
    percentile_good: float = 60.0
    percentile_average: float = 30.0


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


def calculate_percentile(student_marks: float, all_marks: List[float]) -> Optional[float]:
    """
    Calculate percentile rank of a student's marks within a subject.
    
    Args:
        student_marks: The student's marks for the subject
        all_marks: List of all students' marks for the same subject
        
    Returns:
        Percentile rank (0-100) or None if calculation not possible
    """
    if student_marks is None or not all_marks:
        return None
        
    # Remove None values and ensure we have valid marks
    valid_marks = [mark for mark in all_marks if mark is not None]
    if not valid_marks or student_marks not in valid_marks:
        return None
        
    # Count marks below the student's score
    marks_below = sum(1 for mark in valid_marks if mark < student_marks)
    marks_equal = sum(1 for mark in valid_marks if mark == student_marks)
    
    # Standard percentile formula: (marks_below + 0.5 * marks_equal) / total * 100
    total_marks = len(valid_marks)
    percentile = ((marks_below + 0.5 * marks_equal) / total_marks) * 100
    
    return round(percentile, 2)


def calculate_normalized_score(student_marks: float, subject_average: float) -> Optional[float]:
    """
    Calculate normalized score as student_marks / subject_average.
    
    Args:
        student_marks: The student's marks for the subject
        subject_average: Average marks for the subject across all students
        
    Returns:
        Normalized score or None if calculation not possible
    """
    if student_marks is None or subject_average is None or subject_average == 0:
        return None
        
    return round(student_marks / subject_average, 3)


def determine_performance_label(
    percentile: Optional[float], 
    marks: Optional[float], 
    pass_threshold: float = 50.0,
    percentile_excellent: float = 85.0,
    percentile_good: float = 60.0,
    percentile_average: float = 30.0
) -> Optional[str]:
    """
    Determine hybrid performance label based on percentile and threshold combination.
    
    Classification Rules (configurable thresholds):
    - If percentile < percentile_average OR marks < pass_threshold → "At Risk"
    - If percentile between percentile_average–percentile_good → "Average"  
    - If percentile between percentile_good–percentile_excellent → "Good"
    - If percentile > percentile_excellent → "Excellent"
    
    Args:
        percentile: Percentile rank (0-100)
        marks: Student's total marks
        pass_threshold: Minimum passing marks (default: 50.0)
        percentile_excellent: Minimum percentile for "Excellent" (default: 85.0)
        percentile_good: Minimum percentile for "Good" (default: 60.0)
        percentile_average: Minimum percentile for "Average" (default: 30.0)
        
    Returns:
        Performance label string or None
    """
    if percentile is None or marks is None:
        return None
        
    # At Risk: Low percentile OR below pass threshold
    if percentile < percentile_average or marks < pass_threshold:
        return "At Risk"
    elif percentile <= percentile_good:
        return "Average"
    elif percentile <= percentile_excellent:
        return "Good"
    else:
        return "Excellent"


def compute_subject_average(marks_list: List[float]) -> Optional[float]:
    """
    Compute average marks for a subject from list of student marks.
    
    Args:
        marks_list: List of marks from all students for a subject
        
    Returns:
        Average marks or None if no valid marks
    """
    valid_marks = [mark for mark in marks_list if mark is not None]
    if not valid_marks:
        return None
        
    return round(sum(valid_marks) / len(valid_marks), 2)


def compute_hybrid_performance_metrics(
    student_marks: float,
    subject_performance_data: SubjectPerformanceData
) -> HybridPerformanceMetrics:
    """
    Compute comprehensive hybrid performance metrics for a student in a subject.
    
    Args:
        student_marks: The student's total marks for the subject
        subject_performance_data: Performance data for the entire subject including custom thresholds
        
    Returns:
        HybridPerformanceMetrics with percentile, normalized score, and performance label
    """
    all_marks = [marks for _, marks in subject_performance_data.student_marks]
    subject_average = compute_subject_average(all_marks)
    percentile = calculate_percentile(student_marks, all_marks)
    normalized_score = calculate_normalized_score(student_marks, subject_average)
    performance_label = determine_performance_label(
        percentile, 
        student_marks, 
        subject_performance_data.pass_threshold,
        subject_performance_data.percentile_excellent,
        subject_performance_data.percentile_good,
        subject_performance_data.percentile_average
    )
    
    return HybridPerformanceMetrics(
        percentile=percentile,
        normalized_score=normalized_score, 
        performance_label=performance_label,
        subject_average=subject_average
    )

