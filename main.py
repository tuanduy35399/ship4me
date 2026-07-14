# """
# P2P WebRTC File Transfer - FastAPI Signaling Server
# Clean, minimal signaling server for WebRTC offer/answer/ICE exchange.
# Only manages rooms (max 2 peers per room) and forwards JSON messages.
# """

# from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
# from fastapi.responses import FileResponse, HTMLResponse
# from fastapi.staticfiles import StaticFiles
# from fastapi.middleware.cors import CORSMiddleware
# import json
# import logging
# from typing import Dict, List

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# app = FastAPI(title="P2P WebRTC File Transfer Signaling Server")

# # Allow CORS for local dev (adjust in production)
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # Mount static files (index.html + app.js)
# app.mount("/static", StaticFiles(directory="static"), name="static")


# class ConnectionManager:
#     """Quản lý kết nối WebSocket theo phòng (room).
#     Mỗi phòng tối đa 2 client (Sender + Receiver).
#     """

#     def __init__(self):
#         # rooms: { "ROOM123": [websocket1, websocket2] }
#         self.rooms: Dict[str, List[WebSocket]] = {}

#     async def connect(self, websocket: WebSocket, room_id: str) -> bool:
#         """Accept connection. Reject if room already has 2 peers."""
#         await websocket.accept()

#         if room_id not in self.rooms:
#             self.rooms[room_id] = []

#         if len(self.rooms[room_id]) >= 2:
#             logger.warning(f"Room {room_id} is full. Rejecting new connection.")
#             await websocket.close(code=4000, reason="Phòng đã đầy (tối đa 2 thiết bị)")
#             return False

#         self.rooms[room_id].append(websocket)
#         logger.info(f"Client joined room {room_id}. Current count: {len(self.rooms[room_id])}")
#         return True

#     def disconnect(self, websocket: WebSocket, room_id: str):
#         """Remove client from room. Clean up empty rooms."""
#         if room_id in self.rooms and websocket in self.rooms[room_id]:
#             self.rooms[room_id].remove(websocket)
#             logger.info(f"Client left room {room_id}. Remaining: {len(self.rooms[room_id])}")
#             if len(self.rooms[room_id]) == 0:
#                 del self.rooms[room_id]
#                 logger.info(f"Room {room_id} deleted (empty)")

#     async def broadcast_to_room(self, message: str, room_id: str, exclude: WebSocket = None):
#         """Gửi message đến tất cả (hoặc trừ exclude) trong phòng."""
#         if room_id not in self.rooms:
#             return
#         for connection in self.rooms[room_id]:
#             if connection != exclude:
#                 try:
#                     await connection.send_text(message)
#                 except Exception as e:
#                     logger.error(f"Failed to send to peer in {room_id}: {e}")

#     async def notify_peer_joined(self, room_id: str):
#         """Khi phòng đủ 2 người, thông báo cho cả 2 để bắt đầu WebRTC handshake."""
#         if room_id not in self.rooms or len(self.rooms[room_id]) != 2:
#             return
#         message = json.dumps({"type": "peer-joined", "room_id": room_id})
#         for connection in self.rooms[room_id]:
#             try:
#                 await connection.send_text(message)
#             except Exception as e:
#                 logger.error(f"Notify peer-joined failed: {e}")


# manager = ConnectionManager()


# @app.get("/", response_class=FileResponse)
# async def serve_index():
#     """Serve the main frontend page."""
#     return FileResponse("static/index.html")


# @app.websocket("/ws/{room_id}")
# async def websocket_endpoint(websocket: WebSocket, room_id: str):
#     """WebSocket signaling endpoint.
#     - Client connect với room_id trên URL path.
#     - Server chỉ forward các message (offer, answer, ice_candidate, join) giữa 2 peer.
#     - Tự động reject nếu phòng đã đầy.
#     """
#     accepted = await manager.connect(websocket, room_id)
#     if not accepted:
#         return

#     # Nếu vừa đủ 2 người → thông báo peer-joined để trigger WebRTC
#     if len(manager.rooms.get(room_id, [])) == 2:
#         await manager.notify_peer_joined(room_id)

#     try:
#         while True:
#             data = await websocket.receive_text()
#             # Forward raw message to the other peer in the same room
#             await manager.broadcast_to_room(data, room_id, exclude=websocket)

#     except WebSocketDisconnect:
#         logger.info(f"WebSocket disconnected from room {room_id}")
#         manager.disconnect(websocket, room_id)
#         # Optional: notify the remaining peer
#         if room_id in manager.rooms and len(manager.rooms[room_id]) == 1:
#             try:
#                 await manager.rooms[room_id][0].send_text(
#                     json.dumps({"type": "peer-disconnected"})
#                 )
#             except Exception:
#                 pass
#     except Exception as e:
#         logger.error(f"Unexpected error in WS {room_id}: {e}")
#         manager.disconnect(websocket, room_id)


# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)

"""
P2P WebRTC File Transfer - FastAPI Signaling Server (Multi-peer)
Supports multiple peers per room. Broadcasts messages to all other peers.
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import json
import logging
from typing import Dict, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="P2P WebRTC File Transfer Signaling Server (Multi-peer)")

# Allow CORS for local dev (adjust in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files (index.html + app.js)
app.mount("/static", StaticFiles(directory="static"), name="static")


class ConnectionManager:
    """Quản lý kết nối WebSocket theo phòng (room).
    Hỗ trợ nhiều client trong cùng một phòng (multi-peer).
    """

    def __init__(self):
        # rooms: { "ROOM123": [websocket1, websocket2, ...] }
        self.rooms: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, room_id: str) -> bool:
        """Accept connection. No limit on number of peers per room."""
        await websocket.accept()

        if room_id not in self.rooms:
            self.rooms[room_id] = []

        self.rooms[room_id].append(websocket)
        current_count = len(self.rooms[room_id])
        logger.info(f"Client joined room {room_id}. Current count: {current_count}")

        # Notify all peers (including the new one) that someone joined
        await self.broadcast_to_room(
            json.dumps({
                "type": "peer-joined",
                "room_id": room_id,
                "count": current_count
            }),
            room_id
        )
        return True

    def disconnect(self, websocket: WebSocket, room_id: str):
        """Remove client from room. Clean up empty rooms."""
        if room_id in self.rooms and websocket in self.rooms[room_id]:
            self.rooms[room_id].remove(websocket)
            remaining = len(self.rooms[room_id])
            logger.info(f"Client left room {room_id}. Remaining: {remaining}")

            if remaining == 0:
                del self.rooms[room_id]
                logger.info(f"Room {room_id} deleted (empty)")
            else:
                # Notify remaining peers
                try:
                    await self.broadcast_to_room(
                        json.dumps({"type": "peer-disconnected", "room_id": room_id, "count": remaining}),
                        room_id
                    )
                except Exception:
                    pass

    async def broadcast_to_room(self, message: str, room_id: str, exclude: WebSocket = None):
        """Gửi message đến tất cả (hoặc trừ exclude) trong phòng."""
        if room_id not in self.rooms:
            return
        for connection in self.rooms[room_id]:
            if connection != exclude:
                try:
                    await connection.send_text(message)
                except Exception as e:
                    logger.error(f"Failed to send to peer in {room_id}: {e}")


manager = ConnectionManager()


@app.get("/", response_class=FileResponse)
async def serve_index():
    """Serve the main frontend page."""
    return FileResponse("static/index.html")


@app.websocket("/ws/{room_id}")
async def websocket_endpoint(websocket: WebSocket, room_id: str):
    """WebSocket signaling endpoint (Multi-peer).
    - Client connect với room_id trên URL path.
    - Server broadcast các message (offer, answer, ice_candidate, join) đến tất cả peer khác trong room.
    - Hỗ trợ nhiều người trong cùng một phòng.
    """
    accepted = await manager.connect(websocket, room_id)
    if not accepted:
        return

    try:
        while True:
            data = await websocket.receive_text()
            # Forward message to all other peers in the room
            await manager.broadcast_to_room(data, room_id, exclude=websocket)

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected from room {room_id}")
        manager.disconnect(websocket, room_id)
    except Exception as e:
        logger.error(f"Unexpected error in WS {room_id}: {e}")
        manager.disconnect(websocket, room_id)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
