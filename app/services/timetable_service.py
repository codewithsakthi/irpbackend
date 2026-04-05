from __future__ import annotations

from typing import Dict, Iterable, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .. import models


async def get_faculty_timetable(
    db: AsyncSession, faculty_id: int, section: Optional[str] = None, semester: Optional[int] = None
) -> List[dict]:
    """
    Fetch timetable rows for a faculty member from the database.
    Returns an empty list if no entries are found.
    """
    q = (
        select(models.TimeTable, models.Subject, models.Staff)
        .join(models.Subject, models.TimeTable.subject_id == models.Subject.id)
        .outerjoin(models.Staff, models.TimeTable.faculty_id == models.Staff.id)
        .filter(models.TimeTable.faculty_id == faculty_id)
    )
    if section:
        q = q.filter(models.TimeTable.section == section)
    if semester:
        q = q.filter(models.TimeTable.semester == semester)

    result = await db.execute(q)
    rows = result.all()
    
    if not rows:
        return []

    return [
        {
            "id": tt.id,
            "day_of_week": tt.day_of_week,
            "period": tt.period,  # Fixed: was tt.hour, now tt.period
            "subject_id": tt.subject_id,
            "subject_name": subj.name,
            "course_code": subj.course_code,
            "section": tt.section,
            "semester": tt.semester,
        }
        for tt, subj, _staff in rows
    ]


async def get_section_timetable(
    db: AsyncSession, section: Optional[str] = None, semester: Optional[int] = None
) -> List[dict]:
    """
    Fetch timetable rows for a given section from the database.
    Returns an empty list if no entries are found.
    """
    section_key = (section or "A").upper()
    q = (
        select(models.TimeTable, models.Subject, models.Staff)
        .join(models.Subject, models.TimeTable.subject_id == models.Subject.id)
        .outerjoin(models.Staff, models.TimeTable.faculty_id == models.Staff.id)
        .filter(models.TimeTable.section == section_key)
    )
    if semester:
        q = q.filter(models.TimeTable.semester == semester)

    result = await db.execute(q)
    rows = result.all()
    
    if not rows:
        return []

    return [
        {
            "id": tt.id,
            "day_of_week": tt.day_of_week,
            "period": tt.period,  # Fixed: was tt.hour, now tt.period
            "subject_id": tt.subject_id,
            "subject_name": subj.name,
            "course_code": subj.course_code,
            "section": tt.section,
            "semester": tt.semester,
        }
        for tt, subj, _staff in rows
    ]
