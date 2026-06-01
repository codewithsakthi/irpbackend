import os
from datetime import datetime, date
from typing import List, Optional
from pathlib import Path as pyPath

from fastapi import APIRouter, Depends, HTTPException, Query, Path, UploadFile, File, Form
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete, func
from sqlalchemy.orm import joinedload

from ...core import auth
from ...core.database import get_db
from ...models import base as models
from ...schemas import base as schemas
from .websocket import broadcast_achievement

router = APIRouter(tags=["Achievements & Publications"])

UPLOAD_DIR = pyPath("uploads/achievements")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# Helper to secure paths
def _secure_attachment_path(achievement_id: int, filename: str) -> pyPath:
    achievement_folder = UPLOAD_DIR / str(achievement_id)
    achievement_folder.mkdir(parents=True, exist_ok=True)
    return achievement_folder / filename


@router.post(
    "",
    response_model=schemas.AchievementResponse,
    summary="Create & Broadcast Achievement",
    description="Students, staff, or admins upload achievements/journals/publications. Broadcasts real-time alert to all."
)
async def create_achievement(
    title: str = Form(...),
    description: Optional[str] = Form(None),
    achievement_type: str = Form("achievement"),
    date_achieved_str: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    parsed_date = None
    if date_achieved_str:
        try:
            parsed_date = datetime.strptime(date_achieved_str, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

    # Get creator name
    creator_name = current_user.username
    creator_role = current_user.role.name if current_user.role else "user"
    profile_photo = None

    if creator_role.lower() in {"staff", "faculty", "hod", "director"}:
        staff_res = await db.execute(select(models.Staff).where(models.Staff.id == current_user.id))
        staff = staff_res.scalars().first()
        if staff:
            creator_name = staff.name
        prof_res = await db.execute(select(models.StaffProfile).where(models.StaffProfile.staff_id == current_user.id))
        prof = prof_res.scalars().first()
        if prof and prof.profile_photo_url:
            profile_photo = prof.profile_photo_url
    elif creator_role.lower() == "student":
        student_res = await db.execute(select(models.Student).where(models.Student.id == current_user.id))
        student = student_res.scalars().first()
        if student:
            creator_name = student.name
    elif creator_role.lower() == "admin":
        creator_name = "System Administrator"

    # Insert Achievement
    achievement = models.Achievement(
        user_id=current_user.id,
        title=title,
        description=description,
        achievement_type=achievement_type,
        date_achieved=parsed_date,
        created_at=datetime.utcnow()
    )
    db.add(achievement)
    await db.flush()

    # Handle file upload if present
    if file:
        content = await file.read()
        if len(content) > 10 * 1024 * 1024:  # 10MB limit
            raise HTTPException(status_code=400, detail="Attachment exceeds 10MB limit.")

        original_ext = pyPath(file.filename).suffix.lower() if file.filename else ".png"
        filename = f"proof{original_ext}"
        dest_path = _secure_attachment_path(achievement.id, filename)
        dest_path.write_bytes(content)

        attachment_url = f"/api/v1/achievements/attachment/{achievement.id}/{filename}"
        achievement.attachment_url = attachment_url

    await db.commit()
    await db.refresh(achievement)

    # 1. Create DB Notifications for all other users (for offline persistence)
    users_res = await db.execute(select(models.User.id).where(models.User.id != current_user.id))
    other_user_ids = users_res.scalars().all()
    
    emoji_map = {
        "journal": "📄 Publication",
        "publication": "📄 Publication",
        "award": "🏆 Award",
        "certification": "📜 Certification",
        "achievement": "🌟 Achievement",
    }
    emoji = emoji_map.get(achievement_type.lower(), "🌟 Achievement")
    
    notif_title = f"{emoji} by {creator_name}"
    notif_content = f"{creator_name} ({creator_role.upper()}) shared a new {achievement_type}: '{title}'"
    
    for uid in other_user_ids:
        db.add(models.Notification(
            recipient_id=uid,
            title=notif_title,
            content=notif_content,
            channel="SYSTEM",
            status="unread",
            priority="normal",
            sender_id=current_user.id,
            created_at=datetime.utcnow()
        ))
    
    await db.commit()

    # 2. Trigger real-time WebSocket broadcast
    await broadcast_achievement(
        title=notif_title,
        message=notif_content,
        meta={
            "achievement_id": achievement.id,
            "achievement_type": achievement_type,
            "user_name": creator_name,
            "user_role": creator_role,
            "title": title
        }
    )

    # 3. Trigger background PWA Web Push notifications to all other users
    try:
        push_stmt = select(models.PushSubscription).where(models.PushSubscription.user_id != current_user.id)
        push_res = await db.execute(push_stmt)
        push_subs = push_res.scalars().all()

        push_payload = {
            "title": notif_title,
            "message": notif_content,
            "url": "/?tab=Achievements"
        }

        for sub in push_subs:
            sub_info = {
                "endpoint": sub.endpoint,
                "keys": {
                    "p256dh": sub.p256dh,
                    "auth": sub.auth
                }
            }
            asyncio.create_task(deliver_push_async(sub_info, push_payload))
    except Exception as push_err:
        print(f"[Push] Failed to fire background push tasks: {push_err}")

    return schemas.AchievementResponse(
        id=achievement.id,
        user_id=achievement.user_id,
        user_name=creator_name,
        user_role=creator_role,
        user_profile_photo=profile_photo,
        title=achievement.title,
        description=achievement.description,
        achievement_type=achievement.achievement_type,
        date_achieved=achievement.date_achieved,
        attachment_url=achievement.attachment_url,
        created_at=achievement.created_at
    )


@router.get(
    "",
    response_model=List[schemas.AchievementResponse],
    summary="Get Achievements Celebration Feed",
    description="Fetch a unified celebration feed of all achievements sorted newest first."
)
async def get_achievements(
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    stmt = (
        select(models.Achievement)
        .options(joinedload(models.Achievement.user).joinedload(models.User.role))
        .order_by(models.Achievement.created_at.desc())
    )
    res = await db.execute(stmt)
    achievements = res.scalars().all()

    response_data = []
    for ach in achievements:
        role_name = ach.user.role.name if ach.user and ach.user.role else "user"
        name = ach.user.username
        profile_photo = None

        if role_name.lower() in {"staff", "faculty", "hod", "director"}:
            staff_res = await db.execute(select(models.Staff).where(models.Staff.id == ach.user_id))
            staff = staff_res.scalars().first()
            if staff:
                name = staff.name
            prof_res = await db.execute(select(models.StaffProfile).where(models.StaffProfile.staff_id == ach.user_id))
            prof = prof_res.scalars().first()
            if prof and prof.profile_photo_url:
                profile_photo = prof.profile_photo_url
        elif role_name.lower() == "student":
            student_res = await db.execute(select(models.Student).where(models.Student.id == ach.user_id))
            student = student_res.scalars().first()
            if student:
                name = student.name
        elif role_name.lower() == "admin":
            name = "System Administrator"

        response_data.append(schemas.AchievementResponse(
            id=ach.id,
            user_id=ach.user_id,
            user_name=name,
            user_role=role_name,
            user_profile_photo=profile_photo,
            title=ach.title,
            description=ach.description,
            achievement_type=ach.achievement_type,
            date_achieved=ach.date_achieved,
            attachment_url=ach.attachment_url,
            created_at=ach.created_at
        ))

    return response_data


@router.delete(
    "/{id}",
    response_model=schemas.MessageResponse,
    summary="Delete Achievement",
    description="Only the owner or an HOD/Admin can delete an achievement."
)
async def delete_achievement(
    id: int = Path(...),
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    res = await db.execute(select(models.Achievement).where(models.Achievement.id == id))
    ach = res.scalars().first()
    if not ach:
        raise HTTPException(status_code=404, detail="Achievement not found.")

    role_name = current_user.role.name.lower() if current_user.role else ""
    is_admin_or_hod = role_name in {"admin", "hod", "director"}

    if ach.user_id != current_user.id and not is_admin_or_hod:
        raise HTTPException(status_code=403, detail="You do not have permission to delete this achievement.")

    # Remove files if present
    achievement_folder = UPLOAD_DIR / str(id)
    if achievement_folder.exists():
        import shutil
        try:
            shutil.rmtree(achievement_folder)
        except Exception:
            pass

    await db.delete(ach)
    await db.commit()
    return schemas.MessageResponse(message="Achievement deleted successfully.")


@router.get(
    "/attachment/{achievement_id}/{filename}",
    summary="Serve Achievement Proof / Paper",
    description="Securely serves an achievement's uploaded proof or paper."
)
async def serve_attachment(
    achievement_id: int = Path(...),
    filename: str = Path(...),
    db: AsyncSession = Depends(get_db)
):
    achievement_folder = (UPLOAD_DIR / str(achievement_id)).resolve()
    full_path = (achievement_folder / filename).resolve()
    
    # Path traversal guard
    base_dir = UPLOAD_DIR.resolve()
    if base_dir not in full_path.parents:
        raise HTTPException(status_code=400, detail="Invalid attachment path.")
        
    if not full_path.exists() or not full_path.is_file():
        raise HTTPException(status_code=404, detail="Attachment file not found.")
        
    return FileResponse(full_path)


# ─── Notifications persistence endpoints ──────────────────────────────────

@router.get(
    "/notifications",
    response_model=List[schemas.NotificationResponse],
    summary="Get Persistent Notifications",
    description="Fetches persistent, unread in-app notifications for the current user."
)
async def get_notifications(
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    stmt = (
        select(models.Notification)
        .where(models.Notification.recipient_id == current_user.id)
        .where(models.Notification.status == "unread")
        .order_by(models.Notification.created_at.desc())
        .limit(50)
    )
    res = await db.execute(stmt)
    notifications = res.scalars().all()
    return notifications


@router.post(
    "/notifications/mark-read",
    response_model=schemas.MessageResponse,
    summary="Mark All Notifications Read",
    description="Bulk updates all unread notifications to 'read' state."
)
async def mark_notifications_read(
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    await db.execute(
        update(models.Notification)
        .where(models.Notification.recipient_id == current_user.id)
        .where(models.Notification.status == "unread")
        .values(status="read", read_at=datetime.utcnow())
    )
    await db.commit()
    return schemas.MessageResponse(message="All notifications marked as read.")


# ─── PWA Push Notifications subscription management ───────────────────────

@router.get(
    "/push/public-key",
    summary="Get VAPID Public Key",
    description="Returns the VAPID public key so the PWA frontend can subscribe to push notifications."
)
async def get_vapid_public_key():
    try:
        with open("vapid_keys.json", "r") as f:
            keys = json.load(f)
        return {"public_key": keys["public_key"]}
    except Exception:
        # Fallback public key
        return {"public_key": "BE19OLksLmEL24YQxdue8vspBVHEk9A_BrS68GZhBH6AqUXJwi0yPuNCKVrD-N15YBDFpU-eLhXh19pVD93SP_4"}


@router.post(
    "/push/subscribe",
    response_model=schemas.MessageResponse,
    summary="Subscribe to Push Notifications",
    description="Saves a user's PWA push subscription credentials in the database."
)
async def subscribe_push(
    payload: schemas.PushSubscriptionCreate,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    # Check if subscription already exists
    existing = await db.execute(
        select(models.PushSubscription)
        .where(models.PushSubscription.endpoint == payload.endpoint)
    )
    sub = existing.scalars().first()

    if sub:
        # Update keys
        sub.p256dh = payload.keys.p256dh
        sub.auth = payload.keys.auth
        sub.user_id = current_user.id
    else:
        # Create new
        sub = models.PushSubscription(
            user_id=current_user.id,
            endpoint=payload.endpoint,
            p256dh=payload.keys.p256dh,
            auth=payload.keys.auth,
            created_at=datetime.utcnow()
        )
        db.add(sub)

    await db.commit()
    return schemas.MessageResponse(message="Subscribed to background push notifications successfully.")


@router.post(
    "/push/unsubscribe",
    response_model=schemas.MessageResponse,
    summary="Unsubscribe from Push Notifications",
    description="Removes a user's push subscription from the database."
)
async def unsubscribe_push(
    payload: dict,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    endpoint = payload.get("endpoint")
    if not endpoint:
        raise HTTPException(status_code=400, detail="endpoint is required")

    await db.execute(
        delete(models.PushSubscription)
        .where(models.PushSubscription.endpoint == endpoint)
    )
    await db.commit()
    return schemas.MessageResponse(message="Unsubscribed from background push notifications successfully.")


# ─── Push Delivery Helpers ────────────────────────────────────────────────

import asyncio
import json
from pywebpush import webpush, WebPushException

import os

def send_web_push(subscription_info: dict, payload: dict):
    endpoint = subscription_info.get("endpoint", "")
    short_endpoint = endpoint[:40] + "..." if len(endpoint) > 40 else endpoint
    print(f"[Push] Initiating web push delivery to endpoint: {short_endpoint}")
    
    try:
        with open("vapid_keys.json", "r") as f:
            keys = json.load(f)
    except Exception:
        print("[Push] Error: VAPID keys file (vapid_keys.json) not found.")
        return False

    # Automatically write private_key.pem if it does not exist (Self-healing)
    pem_path = "private_key.pem"
    if not os.path.exists(pem_path):
        try:
            with open(pem_path, "w") as f_pem:
                f_pem.write(keys["private_key"])
            print("[Push] Self-healed: Wrote private key to private_key.pem")
        except Exception as pem_err:
            print(f"[Push] Error writing private_key.pem: {pem_err}")
            return False

    try:
        webpush(
            subscription_info=subscription_info,
            data=json.dumps(payload),
            vapid_private_key=pem_path,
            vapid_claims={"sub": "mailto:admin@spark.edu"},
        )
        print(f"[Push] Successfully delivered web push to: {short_endpoint}")
        return True
    except WebPushException as ex:
        # If subscription is expired or invalid, delete it from DB (clean up)
        if ex.response is not None and ex.response.status_code in {404, 410}:
            print(f"[Push] Expired or invalid subscription (status {ex.response.status_code}). Endpoint: {endpoint}")
        else:
            print(f"[Push] Web Push delivery failed with exception: {ex}")
        return False


async def deliver_push_async(sub_info: dict, payload: dict):
    print(f"[Push] Dispatching async push worker to thread pool executor...")
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, send_web_push, sub_info, payload)
