"""SPICS — Profile completion score calculator and file utilities"""
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

UPLOAD_BASE = Path(os.getenv("SPICS_UPLOAD_PATH", "./uploads"))


# ── Completion Score ───────────────────────────────────────────────────────────

def compute_completion_score(
    profile,
    project_count: int = 0,
    skill_count: int = 0,
    cert_count: int = 0,
) -> float:
    """
    Computes a 0-100 profile completion score based on filled fields.
    Points breakdown (total = 100):
      Core identity  (40pts): bio, domain, github, linkedin, portfolio
      Social links   (10pts): leetcode/hackerrank/codechef
      Projects       (20pts): up to 4 projects (5pts each)
      Skills         (15pts): up to 5 skills (3pts each)
      Certifications (10pts): up to 2 certs (5pts each)
      Resume         (5pts):  resume uploaded
    """
    score = 0.0

    # Core identity (40 pts)
    if profile.bio:             score += 10
    if profile.primary_domain:  score += 10
    if profile.github_username: score += 10
    if profile.linkedin_url:    score += 5
    if profile.portfolio_url:   score += 5

    # Social platforms (10 pts)
    platforms = [profile.leetcode_username, profile.hackerrank_username, profile.codechef_username]
    filled = sum(1 for p in platforms if p)
    score += min(filled * 4, 10)

    # Projects (20 pts — 5 per project, max 4)
    score += min(project_count * 5, 20)

    # Skills (15 pts — 3 per skill, max 5)
    score += min(skill_count * 3, 15)

    # Certifications (10 pts — 5 per cert, max 2)
    score += min(cert_count * 5, 10)

    # Resume (5 pts)
    if profile.resume_file_path: score += 5

    return round(min(score, 100.0), 2)


# ── File Utilities ─────────────────────────────────────────────────────────────

def get_resume_upload_dir(student_id: int) -> Path:
    path = UPLOAD_BASE / "resumes" / str(student_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_cert_upload_dir(student_id: int) -> Path:
    path = UPLOAD_BASE / "certifications" / str(student_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_picture_upload_dir(student_id: int) -> Path:
    path = UPLOAD_BASE / "pictures" / str(student_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def save_upload(file_content: bytes, dest_path: Path) -> None:
    dest_path.write_bytes(file_content)


def delete_file_if_exists(file_path: Optional[str]) -> None:
    if file_path:
        try:
            Path(file_path).unlink(missing_ok=True)
        except Exception:
            pass
