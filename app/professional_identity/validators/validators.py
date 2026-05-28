"""SPICS validators — URL and file validation"""
import re
from typing import Optional
from fastapi import HTTPException


_GITHUB_RE  = re.compile(r"^(https?://)?github\.com/[\w\-\.]+/?$", re.I)
_LINKEDIN_RE = re.compile(r"^(https?://)?(www\.)?linkedin\.com/in/[\w\-]+/?$", re.I)
_URL_RE     = re.compile(
    r"^(https?://)"
    r"(([a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,})"
    r"(/[^\s]*)?$"
)

ALLOWED_MIME_TYPES = {
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/msword",
}
ALLOWED_EXTENSIONS = {".pdf", ".docx", ".doc"}
MAX_FILE_SIZE_BYTES = 5 * 1024 * 1024  # 5 MB


def validate_github_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    if not _GITHUB_RE.match(url):
        raise HTTPException(status_code=422, detail=f"Invalid GitHub URL: {url}")
    if not url.startswith("http"):
        url = "https://" + url
    return url.rstrip("/")


def validate_generic_url(url: Optional[str], field_name: str = "URL") -> Optional[str]:
    if not url:
        return None
    if not _URL_RE.match(url):
        raise HTTPException(status_code=422, detail=f"Invalid {field_name}: {url}")
    return url


def validate_upload_file(filename: str, content_type: str, size_bytes: int) -> None:
    """Validates an uploaded file for type and size constraints."""
    import os
    ext = os.path.splitext(filename.lower())[1]
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid file type '{ext}'. Allowed: {', '.join(ALLOWED_EXTENSIONS)}",
        )
    if content_type not in ALLOWED_MIME_TYPES:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid content type '{content_type}'. Allowed: PDF, DOCX",
        )
    if size_bytes > MAX_FILE_SIZE_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"File too large ({size_bytes // 1024}KB). Maximum allowed: 5MB",
        )
