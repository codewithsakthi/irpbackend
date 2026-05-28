"""SPICS — Resume upload and text extraction service.
Supports PDF (via pypdf) and DOCX (via python-docx).
Text extraction is best-effort — fails gracefully.
"""
import io
import logging
import os
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def extract_text_from_pdf(content: bytes) -> str:
    """Extract text from PDF bytes. Returns empty string on failure."""
    try:
        import pypdf
        reader = pypdf.PdfReader(io.BytesIO(content))
        pages  = [page.extract_text() or "" for page in reader.pages]
        return "\n".join(pages).strip()
    except ImportError:
        logger.warning("pypdf not installed — PDF extraction unavailable")
        return ""
    except Exception as e:
        logger.warning(f"PDF extraction failed: {e}")
        return ""


def extract_text_from_docx(content: bytes) -> str:
    """Extract text from DOCX bytes. Returns empty string on failure."""
    try:
        import docx
        doc = docx.Document(io.BytesIO(content))
        return "\n".join(para.text for para in doc.paragraphs if para.text).strip()
    except ImportError:
        logger.warning("python-docx not installed — DOCX extraction unavailable")
        return ""
    except Exception as e:
        logger.warning(f"DOCX extraction failed: {e}")
        return ""


def extract_resume_text(content: bytes, filename: str) -> str:
    """Dispatch to correct extractor based on file extension."""
    ext = Path(filename).suffix.lower()
    if ext == ".pdf":
        return extract_text_from_pdf(content)
    elif ext in (".docx", ".doc"):
        return extract_text_from_docx(content)
    return ""


async def save_resume_file(
    student_id: int,
    filename: str,
    content: bytes,
) -> str:
    """
    Saves resume file to upload directory.
    Returns the relative file path stored in DB.
    """
    from ..utils.utils import get_resume_upload_dir
    upload_dir = get_resume_upload_dir(student_id)
    # Use timestamp prefix to avoid collisions
    timestamp  = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe_name  = f"{timestamp}_{Path(filename).name}"
    dest_path  = upload_dir / safe_name
    dest_path.write_bytes(content)
    # Return relative path (for portability)
    rel_path = str(Path("uploads") / "resumes" / str(student_id) / safe_name)
    logger.info(f"Resume saved: {rel_path}")
    return rel_path
