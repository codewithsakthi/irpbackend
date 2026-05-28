"""SPICS — GitHub public API integration service.
Uses ONLY official GitHub REST API v3 (no HTML scraping, no browser automation).
Results are cached in the student profile row (1-hour TTL).
"""
import logging
import os
from collections import Counter
from datetime import datetime, timedelta
from typing import Optional

import httpx

from sqlalchemy.ext.asyncio import AsyncSession
from ..repositories.profile_repo import ProfileRepository

logger = logging.getLogger(__name__)

GITHUB_API_BASE = "https://api.github.com"
GITHUB_TOKEN    = os.getenv("GITHUB_TOKEN")  # optional — raises rate limit from 60→5000/hr
CACHE_TTL_HOURS = int(os.getenv("SPICS_GITHUB_CACHE_HOURS", "1"))


def _headers(token: Optional[str] = None) -> dict:
    h = {
        "Accept":     "application/vnd.github+json",
        "User-Agent": "SPARK-SPICS/1.0",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    elif GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h


async def _gh_get(client: httpx.AsyncClient, path: str, token: Optional[str] = None) -> Optional[dict | list]:
    try:
        resp = await client.get(
            f"{GITHUB_API_BASE}{path}",
            headers=_headers(token),
            timeout=10.0,
        )
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return None
        logger.warning(f"GitHub API {path} → {e.response.status_code}")
        return None
    except Exception as e:
        logger.warning(f"GitHub API error for {path}: {e}")
        return None


async def fetch_github_analysis(
    student_id: int,
    github_username: str,
    db: AsyncSession,
    force_refresh: bool = False,
) -> dict:
    """
    Fetches GitHub stats via official API. Returns cached result if < CACHE_TTL_HOURS old.
    """
    repo = ProfileRepository(db)
    profile = await repo.get_by_student_id(student_id)
    if not profile:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Student profile not found. Create a profile first.")

    github_token = profile.github_access_token

    # Check cache
    if not force_refresh and profile.github_cache_data and profile.github_cache_expires_at:
        cached = profile.github_cache_data
        if cached.get("github_username") == github_username:
            expires_at = profile.github_cache_expires_at
            if isinstance(expires_at, str):
                expires_at = datetime.fromisoformat(expires_at)
            if expires_at > datetime.utcnow():
                logger.info(f"GitHub cache hit for student {student_id}")
                cached["cached"] = True
                return cached

    async with httpx.AsyncClient() as client:
        if github_token:
            user_data = await _gh_get(client, "/user", token=github_token)
            if not user_data:
                user_data = await _gh_get(client, f"/users/{github_username}")
        else:
            user_data = await _gh_get(client, f"/users/{github_username}")
        if not user_data:
            # Do NOT fabricate or return synthetic GitHub data.
            logger.info(f"GitHub API returned no data for user @{github_username}; skipping synthetic fallback.")
            # Let callers handle the absence of GitHub data; return an explicit error object.
            return {"error": "github_user_not_found_or_unavailable"}

        if github_token:
            repos_path = "/user/repos?per_page=100&sort=updated&visibility=all&affiliation=owner,collaborator,organization_member"
            repos_data = await _gh_get(client, repos_path, token=github_token)
            if repos_data is None:
                repos_data = await _gh_get(client, f"/users/{github_username}/repos?per_page=100&sort=updated")
        else:
            repos_data = await _gh_get(client, f"/users/{github_username}/repos?per_page=100&sort=updated")
        repos_data = repos_data or []
        events     = await _gh_get(client, f"/users/{github_username}/events/public?per_page=30") or []

    # Language diversity
    lang_counter: Counter = Counter()
    total_stars = 0
    top_repos = []
    for repo_item in repos_data:
        if isinstance(repo_item, dict):
            lang = repo_item.get("language")
            if lang:
                lang_counter[lang] += 1
            total_stars += repo_item.get("stargazers_count", 0)
            top_repos.append({
                "name":        repo_item.get("name"),
                "description": repo_item.get("description"),
                "language":    lang,
                "stars":       repo_item.get("stargazers_count", 0),
                "forks":       repo_item.get("forks_count", 0),
                "url":         repo_item.get("html_url"),
                "updated_at":  repo_item.get("updated_at"),
            })

    top_repos.sort(key=lambda r: (r.get("stars", 0), r.get("forks", 0)), reverse=True)
    top_repos = top_repos[:6]

    # Contribution activity (last 30 events)
    push_count = sum(1 for e in events if isinstance(e, dict) and e.get("type") == "PushEvent")
    activity_note = (
        f"Active contributor: {push_count} pushes in recent activity."
        if push_count > 5 else
        "Limited recent push activity detected."
    )

    result = {
        "student_id":            student_id,
        "github_username":       github_username,
        "public_repos":          user_data.get("public_repos", 0),
        "followers":             user_data.get("followers", 0),
        "following":             user_data.get("following", 0),
        "account_created":       user_data.get("created_at"),
        "top_languages":         dict(lang_counter.most_common(8)),
        "total_stars":           total_stars,
        "top_repos":             top_repos,
        "contribution_activity": activity_note,
        "analysis_note":         f"Analyzed {len(repos_data)} public repositories.",
        "cached":                False,
        "fetched_at":            datetime.utcnow().isoformat(),
    }

    # Cache result in DB
    expires_at = datetime.utcnow() + timedelta(hours=CACHE_TTL_HOURS)
    await repo.update(profile, {
        "github_cache_data":       result,
        "github_cache_expires_at": expires_at,
        "github_username":         github_username,
    })

    return result


async def import_github_projects(
    student_id: int,
    db: AsyncSession,
) -> dict:
    """
    Imports top repositories from a student's cached/fresh GitHub profile
    and creates StudentProject entries for them.
    Deduplicates based on github_url.
    """
    profile = await ProfileRepository(db).get_by_student_id(student_id)
    if not profile or not profile.github_username:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="Please connect your GitHub username first.")

    # 1. Fetch current analysis
    force_refresh = bool(profile.github_access_token)
    analysis = await fetch_github_analysis(student_id, profile.github_username, db, force_refresh=force_refresh)
    if "error" in analysis:
        from fastapi import HTTPException
        raise HTTPException(status_code=424, detail=f"GitHub API Error: {analysis['error']}")

    top_repos = analysis.get("top_repos", [])
    if not top_repos:
        return {"imported": 0, "message": "No repositories found to import."}

    # 2. Get existing projects
    from ..repositories.data_repos import ProjectRepository
    proj_repo = ProjectRepository(db)
    existing_projects = await proj_repo.list_by_student(student_id)
    existing_urls = {p.github_url for p in existing_projects if p.github_url}

    imported_count = 0
    for repo in top_repos:
        repo_url = repo.get("url")
        if not repo_url or repo_url in existing_urls:
            continue

        # Format title
        title = repo.get("name", "Project").replace("-", " ").replace("_", " ").title()
        
        # Format tech stack
        tech = [repo.get("language")] if repo.get("language") else []

        project_data = {
            "title": title,
            "description": repo.get("description") or f"Public GitHub repository for {title}.",
            "tech_stack": tech,
            "github_url": repo_url,
            "is_github_imported": True,
            "role": "Lead Developer",
            "team_size": 1,
            "complexity_level": "intermediate",
            "completion_status": "completed"
        }
        await proj_repo.create(student_id, project_data)
        imported_count += 1

    # 3. Refresh profile completion score
    if imported_count > 0:
        from .services import ProfileService
        await ProfileService._refresh_profile_completion(student_id, db)

    return {
        "imported": imported_count,
        "message": f"Successfully imported {imported_count} GitHub repositories as projects."
    }

