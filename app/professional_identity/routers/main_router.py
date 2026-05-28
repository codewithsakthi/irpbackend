"""SPICS — Main router aggregating all sub-routers under /api/v1/professional"""
from fastapi import APIRouter

from .core_routers import (
    profile_router, project_router, cert_router,
    skill_router, readiness_router, resume_router,
)
from .extended_routers import github_router, insights_router, faculty_router, leetcode_router

router = APIRouter()

router.include_router(profile_router)
router.include_router(project_router)
router.include_router(cert_router)
router.include_router(skill_router)
router.include_router(readiness_router)
router.include_router(resume_router)
router.include_router(github_router)
router.include_router(leetcode_router)
router.include_router(insights_router)
router.include_router(faculty_router)
