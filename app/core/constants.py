GRADE_POINTS = {
    'O': 10,
    'S': 10,
    'A+': 9,
    'A': 8,
    'B+': 7,
    'B': 6,
    'C': 5,
    'D': 4,
    'E': 3,
    'PASS': 5,
    'P': 5,
    'FAIL': 0,
    'F': 0,
    'U': 0,
    'W': 0,
    'I': 0,
    'AB': 0,
}

DIRECTORY_SORT_KEYS = {
    'roll_no': lambda item: item.roll_no or '',
    'name': lambda item: (item.name or '').lower(),
    'city': lambda item: (item.city or '').lower(),
    'batch': lambda item: (item.batch or '').lower(),
    'semester': lambda item: item.current_semester or 0,
    'gpa': lambda item: item.average_grade_points or 0,
    'internal': lambda item: item.average_internal_percentage or 0,
    'rank': lambda item: item.rank or 999999,
    'attendance': lambda item: item.attendance_percentage or 0,
}

CURRICULUM_CREDITS = {
    "24FC101": 4.0, "24MC103": 3.0, "24MC105": 3.0, "24MC201": 3.0,
    "24MC203": 3.0, "24MC204": 4.0, "24MC2L3": 2.0, "24MC301": 3.0,
    "24MC302": 3.0, "24MC303": 3.0, "24MC304": 3.0, "24MC3L2": 2.0,
    "24MC3L3": 1.0, "24MC4L1": 12.0, "24MCBC1": 3.0, "24MCBC2": 3.0,
    "24MCBC4": 3.0, "24MCBC5": 3.0, "24MCBC6": 3.0, "24MC102": 3.0,
    "24MC1L1": 1.5, "24MC202": 4.0, "24RM101": 3.0, "24MC2E1": 3.0,
    "24MC2E2": 3.0, "24MC2E4": 3.0, "24MC2E6": 3.0, "24MC2E7": 3.0,
    "24MC2E8": 3.0, "24MC3E2": 3.0, "24MC3E3": 3.0, "24MC3E4": 3.0,
    "24MCOE1": 3.0, "24MCOE4": 3.0, "24AC101": 0.0, "24AC102": 0.0,
    "24AC103": 0.0, "24AC104": 0.0, "24AC105": 0.0, "24AC106": 0.0,
    "24AC107": 0.0, "24MC104": 3.0, "24MC1L2": 1.5, "24MC1L3": 1.0,
    "24MC2L1": 2.0, "24MC2L2": 2.0, "24MC2L4": 1.0, "24MC3L1": 2.0,
    "24MC2E3": 3.0, "24MC2E5": 3.0, "24MC3E1": 3.0, "24MCOE2": 3.0,
    "24MCOE3": 3.0, "24AC108": 0.0, "24MCBC3": 3.0
}
