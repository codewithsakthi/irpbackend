"""
WebSocket endpoints for real-time updates.
Handles real-time attendance, marks, and notification delivery.
"""

import json
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.core.database import get_db
from app import models, schemas
from app.core import auth

router = APIRouter(prefix="/ws", tags=["websocket"])

# Connection manager for tracking active WebSocket connections
class ConnectionManager:
    def __init__(self):
        self.active_connections: dict = {}  # {roll_no: [websocket1, websocket2, ...]}

    async def connect(self, roll_no: str, websocket: WebSocket):
        """Register a new WebSocket connection for a student."""
        await websocket.accept()
        if roll_no not in self.active_connections:
            self.active_connections[roll_no] = []
        self.active_connections[roll_no].append(websocket)
        print(f"✅ Student {roll_no} connected to real-time attendance. Total: {len(self.active_connections[roll_no])}")

    async def disconnect(self, roll_no: str, websocket: WebSocket):
        """Unregister a WebSocket connection."""
        if roll_no in self.active_connections:
            self.active_connections[roll_no].remove(websocket)
            if not self.active_connections[roll_no]:
                del self.active_connections[roll_no]
                print(f"❌ Student {roll_no} disconnected from real-time attendance")

    async def broadcast_to_student(self, roll_no: str, message: dict):
        """Send a message to all connections of a specific student."""
        if roll_no in self.active_connections:
            disconnected = []
            for connection in self.active_connections[roll_no]:
                try:
                    await connection.send_json(message)
                except Exception as e:
                    print(f"⚠️  Failed to send message to {roll_no}: {e}")
                    disconnected.append(connection)
            
            # Clean up disconnected connections
            for conn in disconnected:
                await self.disconnect(roll_no, conn)

    async def broadcast_to_all_students(self, message: dict):
        """Broadcast a message to all connected students (admin/announcements)."""
        disconnected = []
        for roll_no, connections in list(self.active_connections.items()):
            for connection in connections:
                try:
                    await connection.send_json(message)
                except Exception:
                    disconnected.append((roll_no, connection))
        
        # Clean up disconnected connections
        for roll_no, conn in disconnected:
            await self.disconnect(roll_no, conn)


manager = ConnectionManager()


@router.websocket("/attendance/{roll_no}")
async def websocket_attendance(websocket: WebSocket, roll_no: str):
    """
    WebSocket endpoint for real-time attendance updates.
    Clients connect here and receive attendance changes instantly.
    
    URL: ws://localhost:8000/api/v1/ws/attendance/{roll_no}
    """
    await manager.connect(roll_no, websocket)
    
    try:
        while True:
            # Keep connection alive and listen for close signals
            try:
                data = await websocket.receive_text()
                # Optionally handle ping/pong or other client messages
                if data == "ping":
                    await websocket.send_text("pong")
            except WebSocketDisconnect:
                break
    except Exception as e:
        print(f"❌ WebSocket error for {roll_no}: {e}")
    finally:
        await manager.disconnect(roll_no, websocket)


async def notify_student_attendance_updated(roll_no: str, message: str):
    """
    Utility function to notify a student that their attendance has been updated.
    Called from the staff attendance submission endpoint.
    """
    await manager.broadcast_to_student(
        roll_no,
        {
            "type": "attendance_update",
            "message": message,
            "timestamp": str(__import__('datetime').datetime.utcnow().isoformat()),
        }
    )


async def notify_all_students(title: str, message: str):
    """Send a broadcast notification to all connected students."""
    await manager.broadcast_to_all_students(
        {
            "type": "announcement",
            "title": title,
            "message": message,
            "timestamp": str(__import__('datetime').datetime.utcnow().isoformat()),
        }
    )
