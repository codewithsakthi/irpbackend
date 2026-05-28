import logging
import httpx
from datetime import datetime, timedelta
from typing import Optional
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from ...models import base as _core_models  # noqa: F401  # registers core tables/metadata
from ..repositories.profile_repo import ProfileRepository

logger = logging.getLogger(__name__)

LEETCODE_STATS_API = "https://leetcode-stats-api.herokuapp.com"
LEETCODE_GRAPHQL_API = "https://leetcode.com/graphql"
CACHE_TTL_HOURS = 2

LEETCODE_GRAPHQL_QUERY = """
query getUserProfile($username: String!) {
    allQuestionsCount {
        difficulty
        count
    }
    matchedUser(username: $username) {
        username
        profile {
            ranking
            reputation
        }
        submitStatsGlobal {
            acSubmissionNum {
                difficulty
                count
                submissions
            }
            totalSubmissionNum {
                difficulty
                count
                submissions
            }
        }
    }
}
"""


def _as_int(value, default: int = 0) -> int:
        try:
                if value is None:
                        return default
                if isinstance(value, bool):
                        return int(value)
                if isinstance(value, (int, float)):
                        return int(value)
                return int(str(value).strip())
        except Exception:
                return default


def _difficulty_count(items: list | None, difficulty: str) -> int:
        if not items:
                return 0
        for item in items:
                if isinstance(item, dict) and item.get("difficulty") == difficulty:
                        return _as_int(item.get("count"), 0)
        return 0


def _build_result(
        student_id: int,
        leetcode_username: str,
        total_solved: int,
        total_questions: int,
        easy_solved: int,
        medium_solved: int,
        hard_solved: int,
        acceptance_rate: float,
        ranking: int,
        contribution_points: int,
        reputation: int,
) -> dict:
        return {
                "student_id": student_id,
                "leetcode_username": leetcode_username,
                "total_solved": total_solved,
                "total_questions": total_questions,
                "easy_solved": easy_solved,
                "medium_solved": medium_solved,
                "hard_solved": hard_solved,
                "acceptance_rate": round(float(acceptance_rate), 2),
                "ranking": ranking,
                "contribution_points": contribution_points,
                "reputation": reputation,
                "cached": False,
                "fetched_at": datetime.utcnow().isoformat(),
                "is_mock": False,
        }

async def fetch_leetcode_analysis(
    student_id: int,
    leetcode_username: str,
    db: AsyncSession,
    force_refresh: bool = False,
) -> dict:
    """
    Fetches LeetCode statistics via public unofficial API.
    Returns real data only; never fabricates fallback stats.
    Caches result in database for CACHE_TTL_HOURS.
    """
    repo = ProfileRepository(db)
    profile = await repo.get_by_student_id(student_id)
    if not profile:
        raise HTTPException(status_code=404, detail="Student profile not found. Create a profile first.")

    # Check cache
    if not force_refresh and profile.leetcode_cache_data and profile.leetcode_cache_expires_at:
        cached = profile.leetcode_cache_data
        if cached.get("leetcode_username") == leetcode_username and not cached.get("is_mock", False):
            expires_at = profile.leetcode_cache_expires_at
            if isinstance(expires_at, str):
                expires_at = datetime.fromisoformat(expires_at)
            if expires_at > datetime.utcnow():
                logger.info(f"LeetCode cache hit for student {student_id}")
                cached["cached"] = True
                return cached

    # Attempt to fetch real stats from primary API, then fallback to official LeetCode GraphQL.
    last_error: Optional[str] = None
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{LEETCODE_STATS_API}/{leetcode_username}", timeout=8.0)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "success":
                    result = _build_result(
                        student_id=student_id,
                        leetcode_username=leetcode_username,
                        total_solved=_as_int(data.get("totalSolved"), 0),
                        total_questions=_as_int(data.get("totalQuestions"), 0),
                        easy_solved=_as_int(data.get("easySolved"), 0),
                        medium_solved=_as_int(data.get("mediumSolved"), 0),
                        hard_solved=_as_int(data.get("hardSolved"), 0),
                        acceptance_rate=float(data.get("acceptanceRate") or 0),
                        ranking=_as_int(data.get("ranking"), 0),
                        contribution_points=_as_int(data.get("contributionPoints"), 0),
                        reputation=_as_int(data.get("reputation"), 0),
                    )
                    # Update profile cache
                    await _update_profile_cache(repo, profile, result, leetcode_username)
                    return result
                api_message = data.get("message") or "LeetCode API returned non-success response."
                if "not exist" in api_message.lower() or "doesn't exist" in api_message.lower():
                    raise HTTPException(status_code=404, detail=f"LeetCode user '{leetcode_username}' not found.")
                last_error = api_message
            elif resp.status_code == 404:
                raise HTTPException(status_code=404, detail=f"LeetCode user '{leetcode_username}' not found.")
            else:
                last_error = f"LeetCode API returned HTTP {resp.status_code}."
        except Exception as e:
            logger.warning(f"Failed to fetch real LeetCode stats for {leetcode_username}: {e}")
            if isinstance(e, HTTPException):
                raise
            last_error = str(e)

        # Secondary source: LeetCode GraphQL (real source, no mock data).
        try:
            gql_resp = await client.post(
                LEETCODE_GRAPHQL_API,
                json={
                    "query": LEETCODE_GRAPHQL_QUERY,
                    "variables": {"username": leetcode_username},
                },
                headers={
                    "Content-Type": "application/json",
                    "Referer": "https://leetcode.com",
                    "User-Agent": "SPARK-SPICS/1.0",
                },
                timeout=10.0,
            )
            if gql_resp.status_code != 200:
                raise RuntimeError(f"LeetCode GraphQL returned HTTP {gql_resp.status_code}")

            gql_data = gql_resp.json()
            if gql_data.get("errors"):
                raise RuntimeError(str(gql_data.get("errors")))

            payload = gql_data.get("data") or {}
            matched_user = payload.get("matchedUser")
            if not matched_user:
                raise HTTPException(status_code=404, detail=f"LeetCode user '{leetcode_username}' not found.")

            all_questions = payload.get("allQuestionsCount") or []
            submit_stats = matched_user.get("submitStatsGlobal") or {}
            ac_submissions = submit_stats.get("acSubmissionNum") or []
            total_submission_num = submit_stats.get("totalSubmissionNum") or []
            profile_data = matched_user.get("profile") or {}

            total_solved = _difficulty_count(ac_submissions, "All")
            easy_solved = _difficulty_count(ac_submissions, "Easy")
            medium_solved = _difficulty_count(ac_submissions, "Medium")
            hard_solved = _difficulty_count(ac_submissions, "Hard")
            total_questions = _difficulty_count(all_questions, "All")

            total_submissions = _difficulty_count(total_submission_num, "All")
            acceptance_rate = 0.0
            if total_submissions > 0:
                acceptance_rate = (total_solved / total_submissions) * 100

            result = _build_result(
                student_id=student_id,
                leetcode_username=leetcode_username,
                total_solved=total_solved,
                total_questions=total_questions,
                easy_solved=easy_solved,
                medium_solved=medium_solved,
                hard_solved=hard_solved,
                acceptance_rate=acceptance_rate,
                ranking=_as_int(profile_data.get("ranking"), 0),
                contribution_points=0,
                reputation=_as_int(profile_data.get("reputation"), 0),
            )

            await _update_profile_cache(repo, profile, result, leetcode_username)
            return result
        except Exception as e:
            logger.warning(f"Failed to fetch LeetCode GraphQL stats for {leetcode_username}: {e}")
            if isinstance(e, HTTPException):
                raise
            if last_error:
                last_error = f"{last_error}; GraphQL fallback failed: {e}"
            else:
                last_error = f"GraphQL fallback failed: {e}"

    raise HTTPException(
        status_code=502,
        detail=(
            "Unable to fetch real LeetCode data right now. "
            f"Reason: {last_error or 'upstream API unavailable'}"
        ),
    )

async def _update_profile_cache(repo, profile, result, leetcode_username):
    expires_at = datetime.utcnow() + timedelta(hours=CACHE_TTL_HOURS)
    await repo.update(profile, {
        "leetcode_cache_data": result,
        "leetcode_cache_expires_at": expires_at,
        "leetcode_username": leetcode_username,
    })
