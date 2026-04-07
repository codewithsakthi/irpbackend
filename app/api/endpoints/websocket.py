"""
WebSocket endpoints for real-time updates.
Handles real-time attendance, marks, and notification delivery.
Supports: student, staff, and admin real-time channels.
"""

import json
from datetime import datetime
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.core.database import get_db
from app import models, schemas
from app.core import auth

router = APIRouter(prefix="/ws", tags=["websocket"])


# ─────────────────────────────────────────────────────────
# Connection Manager
# ─────────────────────────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        # Students: {roll_no: [ws, ...]}
        self.students: dict[str, list[WebSocket]] = {}
        # Staff: {user_id: [ws, ...]}
        self.staff: dict[str, list[WebSocket]] = {}
        # Admins: {user_id: [ws, ...]}
        self.admins: dict[str, list[WebSocket]] = {}

    # ── helpers ──────────────────────────────────────────

    async def _add(self, store: dict, key: str, ws: WebSocket):
        await ws.accept()
        store.setdefault(key, []).append(ws)

    async def _remove(self, store: dict, key: str, ws: WebSocket):
        if key in store:
            try:
                store[key].remove(ws)
            except ValueError:
                pass
            if not store[key]:
                del store[key]

    async def _send(self, store: dict, key: str, message: dict):
        for ws in list(store.get(key, [])):
            try:
                await ws.send_json(message)
            except Exception:
                await self._remove(store, key, ws)

    async def _broadcast(self, store: dict, message: dict):
        for key in list(store.keys()):
            await self._send(store, key, message)

    # ── student ──────────────────────────────────────────

    async def connect_student(self, roll_no: str, ws: WebSocket):
        await self._add(self.students, roll_no, ws)
        print(f"✅ Student {roll_no} connected. Active: {len(self.students.get(roll_no, []))}")

    async def disconnect_student(self, roll_no: str, ws: WebSocket):
        await self._remove(self.students, roll_no, ws)
        print(f"❌ Student {roll_no} disconnected.")

    async def notify_student(self, roll_no: str, message: dict):
        await self._send(self.students, roll_no, message)

    # ── staff ─────────────────────────────────────────────

    async def connect_staff(self, user_id: str, ws: WebSocket):
        await self._add(self.staff, user_id, ws)
        print(f"✅ Staff {user_id} connected to live feed.")

    async def disconnect_staff(self, user_id: str, ws: WebSocket):
        await self._remove(self.staff, user_id, ws)

    async def notify_staff(self, user_id: str, message: dict):
        await self._send(self.staff, user_id, message)

    # ── admin ─────────────────────────────────────────────

    async def connect_admin(self, user_id: str, ws: WebSocket):
        await self._add(self.admins, user_id, ws)
        print(f"✅ Admin {user_id} connected to broadcast feed.")

    async def disconnect_admin(self, user_id: str, ws: WebSocket):
        await self._remove(self.admins, user_id, ws)

    async def broadcast_to_admins(self, message: dict):
        await self._broadcast(self.admins, message)

    async def broadcast_to_all_students(self, message: dict):
        await self._broadcast(self.students, message)


manager = ConnectionManager()


# ─────────────────────────────────────────────────────────
# WebSocket Endpoints
# ─────────────────────────────────────────────────────────

@router.websocket("/attendance/{roll_no}")
async def ws_student(websocket: WebSocket, roll_no: str):
    """Student real-time channel. Receives attendance updates."""
    await manager.connect_student(roll_no, websocket)
    try:
        while True:
            try:
                data = await websocket.receive_text()
                if data == "ping":
                    await websocket.send_text("pong")
            except WebSocketDisconnect:
                break
    except Exception as e:
        print(f"❌ Student WS error {roll_no}: {e}")
    finally:
        await manager.disconnect_student(roll_no, websocket)


@router.websocket("/staff/{user_id}")
async def ws_staff(websocket: WebSocket, user_id: str):
    """Staff real-time channel. Receives submission confirmations and admin broadcasts."""
    await manager.connect_staff(user_id, websocket)
    try:
        while True:
            try:
                data = await websocket.receive_text()
                if data == "ping":
                    await websocket.send_text("pong")
            except WebSocketDisconnect:
                break
    except Exception as e:
        print(f"❌ Staff WS error {user_id}: {e}")
    finally:
        await manager.disconnect_staff(user_id, websocket)


@router.websocket("/admin/{user_id}")
async def ws_admin(websocket: WebSocket, user_id: str):
    """Admin real-time channel. Receives all attendance events across the institution."""
    await manager.connect_admin(user_id, websocket)
    try:
        while True:
            try:
                data = await websocket.receive_text()
                if data == "ping":
                    await websocket.send_text("pong")
            except WebSocketDisconnect:
                break
    except Exception as e:
        print(f"❌ Admin WS error {user_id}: {e}")
    finally:
        await manager.disconnect_admin(user_id, websocket)


# ─────────────────────────────────────────────────────────
# Notification Helpers (called from other endpoints)
# ─────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.utcnow().isoformat() + "Z"


async def notify_student_attendance_updated(roll_no: str, message: str):
    """Notify a specific student that their attendance was updated."""
    await manager.notify_student(
        roll_no,
        {
            "type": "attendance_update",
            "message": message,
            "timestamp": _now(),
        },
    )


async def notify_attendance_broadcast(
    *,
    faculty_id: int,
    faculty_name: str,
    subject_name: str,
    subject_code: str,
    period: int,
    date: str,
    section: str | None,
    semester: int,
    present_count: int,
    absent_count: int,
    od_count: int = 0,
    total_count: int,
    is_substitute: bool = False,
):
    """
    Broadcast an attendance event to:
      1. All connected admins
      2. The submitting staff member (confirmation)
    """
    section_label = f"Sec {section}" if section else "All"
    substitute_note = " (substitute)" if is_substitute else ""

    admin_msg = {
        "type": "attendance_marked",
        "title": f"Attendance Marked — {subject_code}",
        "message": (
            f"{faculty_name}{substitute_note} marked {subject_code} "
            f"[{section_label}, Sem {semester}, P{period}] on {date}: "
            f"{present_count}P / {absent_count}A / {od_count}OD of {total_count} students."
        ),
        "meta": {
            "faculty_id": faculty_id,
            "faculty_name": faculty_name,
            "subject_name": subject_name,
            "subject_code": subject_code,
            "period": period,
            "date": date,
            "section": section,
            "semester": semester,
            "present_count": present_count,
            "absent_count": absent_count,
            "od_count": od_count,
            "total_count": total_count,
            "is_substitute": is_substitute,
        },
        "timestamp": _now(),
    }

    staff_msg = {
        "type": "submission_confirmed",
        "title": "Attendance Submitted",
        "message": (
            f"✅ {subject_code} [{section_label}, P{period}] — "
            f"{present_count} present, {absent_count} absent, {od_count} OD."
        ),
        "timestamp": _now(),
    }

    # Notify all admins
    await manager.broadcast_to_admins(admin_msg)

    # Notify the submitting staff member
    await manager.notify_staff(str(faculty_id), staff_msg)


async def notify_all_students(title: str, message: str):
    """Send a broadcast announcement to all connected students."""
    await manager.broadcast_to_all_students(
        {
            "type": "announcement",
            "title": title,
            "message": message,
            "timestamp": _now(),
        }
    )
