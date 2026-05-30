import { createServer } from "node:http";
import { Server, type Socket } from "socket.io";

type Room = {
  hostId: string;
  viewerId?: string;
  createdAt: number;
};

type SessionDescription = {
  type: string;
  sdp?: string;
};

type IceCandidate = {
  candidate?: string;
  sdpMid?: string | null;
  sdpMLineIndex?: number | null;
  usernameFragment?: string | null;
};

type SignalPayload = {
  roomId: string;
  to?: string;
  sdp?: SessionDescription;
  candidate?: IceCandidate;
};

const port = Number(process.env.PORT ?? process.env.SIGNALING_PORT ?? 4000);
const clientOrigin = process.env.CLIENT_ORIGIN ?? "http://localhost:3000";
const rooms = new Map<string, Room>();

const httpServer = createServer();
const io = new Server(httpServer, {
  cors: {
    origin: clientOrigin.split(",").map((origin) => origin.trim()),
    methods: ["GET", "POST"],
  },
  transports: ["websocket", "polling"],
});

function createRoomId(): string {
  const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let id = "";
  for (let index = 0; index < 6; index += 1) {
    id += alphabet[Math.floor(Math.random() * alphabet.length)];
  }
  return rooms.has(id) ? createRoomId() : id;
}

function roomName(roomId: string) {
  return `room:${roomId}`;
}

function bindSocket(socket: Socket, roomId: string, role: "host" | "viewer") {
  socket.data.roomId = roomId;
  socket.data.role = role;
  socket.join(roomName(roomId));
}

io.on("connection", (socket) => {
  console.log(`[SOCKET CONNECT] Socket ${socket.id} connected`);

  socket.on("room:create", (ack: (response: { roomId: string }) => void) => {
    const roomId = createRoomId();
    rooms.set(roomId, { hostId: socket.id, createdAt: Date.now() });
    bindSocket(socket, roomId, "host");
    console.log(`[ROOM CREATE] Host ${socket.id} created room ${roomId}`);
    ack({ roomId });
  });

  socket.on(
    "room:restore",
    (
      payload: { roomId: string; role: "host" | "viewer" },
      ack: (response: { ok: boolean; error?: string }) => void,
    ) => {
      const roomId = payload.roomId.toUpperCase();
      console.log(`[ROOM RESTORE REQUEST] Socket ${socket.id} attempting to restore as ${payload.role} in room ${roomId}`);

      if (payload.role === "host") {
        rooms.set(roomId, { hostId: socket.id, createdAt: Date.now() });
        bindSocket(socket, roomId, "host");
        console.log(`[ROOM RESTORE SUCCESS] Host ${socket.id} restored room ${roomId}`);
        ack({ ok: true });
        return;
      }

      const room = rooms.get(roomId);
      if (!room) {
        console.log(`[ROOM RESTORE FAIL] Room ${roomId} not found for viewer ${socket.id}`);
        ack({ ok: false, error: "Room is no longer available." });
        return;
      }

      if (room.viewerId && room.viewerId !== socket.id) {
        console.log(`[ROOM RESTORE FAIL] Room ${roomId} already has another viewer (${room.viewerId}) instead of ${socket.id}`);
        ack({ ok: false, error: "This room already has a viewer." });
        return;
      }

      room.viewerId = socket.id;
      bindSocket(socket, roomId, "viewer");
      console.log(`[ROOM RESTORE SUCCESS] Viewer ${socket.id} restored connection in room ${roomId}`);
      io.to(room.hostId).emit("peer:joined", { viewerId: socket.id });
      ack({ ok: true });
    },
  );

  socket.on(
    "room:join",
    (
      payload: { roomId: string },
      ack: (response: { ok: boolean; error?: string }) => void,
    ) => {
      const roomId = payload.roomId.toUpperCase();
      const room = rooms.get(roomId);
      console.log(`[ROOM JOIN REQUEST] Viewer ${socket.id} requesting to join room ${roomId}`);

      if (!room) {
        console.log(`[ROOM JOIN FAIL] Room ${roomId} not found for viewer ${socket.id}`);
        ack({ ok: false, error: "Room not found. Check the invite code." });
        return;
      }

      if (room.viewerId && room.viewerId !== socket.id) {
        console.log(`[ROOM JOIN FAIL] Room ${roomId} already occupied by viewer ${room.viewerId} (requested by ${socket.id})`);
        ack({ ok: false, error: "This room already has a viewer." });
        return;
      }

      room.viewerId = socket.id;
      bindSocket(socket, roomId, "viewer");
      console.log(`[ROOM JOIN SUCCESS] Viewer ${socket.id} joined room ${roomId}`);
      io.to(room.hostId).emit("peer:joined", { viewerId: socket.id });
      ack({ ok: true });
    },
  );

  socket.on("webrtc:offer", (payload: SignalPayload) => {
    if (!payload.to) {
      console.log(`[OFFER DISCARDED] Offer from ${socket.id} has no target 'to' parameter`);
      return;
    }
    console.log(`[OFFER FORWARD] Forwarding WebRTC offer from host ${socket.id} to viewer ${payload.to} for room ${payload.roomId}`);
    io.to(payload.to).emit("webrtc:offer", {
      from: socket.id,
      roomId: payload.roomId,
      sdp: payload.sdp,
    });
  });

  socket.on("webrtc:answer", (payload: SignalPayload) => {
    const room = rooms.get(payload.roomId);
    if (!room) {
      console.log(`[ANSWER DISCARDED] Room ${payload.roomId} not found for answer from ${socket.id}`);
      return;
    }
    console.log(`[ANSWER FORWARD] Forwarding WebRTC answer from viewer ${socket.id} to host ${room.hostId} for room ${payload.roomId}`);
    io.to(room.hostId).emit("webrtc:answer", {
      from: socket.id,
      roomId: payload.roomId,
      sdp: payload.sdp,
    });
  });

  socket.on("webrtc:candidate", (payload: SignalPayload) => {
    const room = rooms.get(payload.roomId);
    if (!room || !payload.candidate) {
      console.log(`[ICE DISCARDED] Room ${payload.roomId} not found or candidate missing in request from ${socket.id}`);
      return;
    }

    const target =
      socket.data.role === "host" ? room.viewerId : room.hostId;

    if (target) {
      console.log(`[ICE FORWARD] Forwarding ICE candidate from ${socket.data.role} ${socket.id} to target ${target} for room ${payload.roomId}`);
      io.to(target).emit("webrtc:candidate", {
        from: socket.id,
        roomId: payload.roomId,
        candidate: payload.candidate,
      });
    } else {
      console.log(`[ICE FORWARD FAIL] Target not available for forwarding candidate from ${socket.data.role} ${socket.id} in room ${payload.roomId}`);
    }
  });

  socket.on("peer:reconnect", (payload: { roomId: string }) => {
    const room = rooms.get(payload.roomId);
    if (!room) {
      console.log(`[RECONNECT FAIL] Room ${payload.roomId} not found for reconnect from ${socket.id}`);
      return;
    }

    console.log(`[PEER RECONNECT] Reconnection request from ${socket.data.role} ${socket.id} for room ${payload.roomId}`);
    if (socket.data.role === "viewer") {
      io.to(room.hostId).emit("peer:joined", { viewerId: socket.id });
    }
  });

  socket.on("disconnect", () => {
    const roomId = socket.data.roomId as string | undefined;
    const role = socket.data.role as "host" | "viewer" | undefined;
    console.log(`[SOCKET DISCONNECT] Socket ${socket.id} disconnected. Role: ${role || "none"}, Room ID: ${roomId || "none"}`);
    if (!roomId || !role) return;

    const room = rooms.get(roomId);
    if (!room) return;

    if (role === "host" && room.hostId === socket.id) {
      console.log(`[ROOM CLOSE] Host disconnected. Closing room ${roomId}`);
      io.to(roomName(roomId)).emit("room:closed");
      rooms.delete(roomId);
      return;
    }

    if (role === "viewer" && room.viewerId === socket.id) {
      console.log(`[ROOM LEAVE] Viewer disconnected from room ${roomId}`);
      room.viewerId = undefined;
      io.to(room.hostId).emit("peer:left");
    }
  });
});

httpServer.listen(port, () => {
  console.log(`StreamLink signaling server listening on :${port}`);
});
