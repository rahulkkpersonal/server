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
  socket.on("room:create", (ack: (response: { roomId: string }) => void) => {
    const roomId = createRoomId();
    rooms.set(roomId, { hostId: socket.id, createdAt: Date.now() });
    bindSocket(socket, roomId, "host");
    ack({ roomId });
  });

  socket.on(
    "room:restore",
    (
      payload: { roomId: string; role: "host" | "viewer" },
      ack: (response: { ok: boolean; error?: string }) => void,
    ) => {
      const roomId = payload.roomId.toUpperCase();

      if (payload.role === "host") {
        rooms.set(roomId, { hostId: socket.id, createdAt: Date.now() });
        bindSocket(socket, roomId, "host");
        ack({ ok: true });
        return;
      }

      const room = rooms.get(roomId);
      if (!room) {
        ack({ ok: false, error: "Room is no longer available." });
        return;
      }

      if (room.viewerId && room.viewerId !== socket.id) {
        ack({ ok: false, error: "This room already has a viewer." });
        return;
      }

      room.viewerId = socket.id;
      bindSocket(socket, roomId, "viewer");
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

      if (!room) {
        ack({ ok: false, error: "Room not found. Check the invite code." });
        return;
      }

      if (room.viewerId && room.viewerId !== socket.id) {
        ack({ ok: false, error: "This room already has a viewer." });
        return;
      }

      room.viewerId = socket.id;
      bindSocket(socket, roomId, "viewer");
      io.to(room.hostId).emit("peer:joined", { viewerId: socket.id });
      ack({ ok: true });
    },
  );

  socket.on("webrtc:offer", (payload: SignalPayload) => {
    if (!payload.to) return;
    io.to(payload.to).emit("webrtc:offer", {
      from: socket.id,
      roomId: payload.roomId,
      sdp: payload.sdp,
    });
  });

  socket.on("webrtc:answer", (payload: SignalPayload) => {
    const room = rooms.get(payload.roomId);
    if (!room) return;
    io.to(room.hostId).emit("webrtc:answer", {
      from: socket.id,
      roomId: payload.roomId,
      sdp: payload.sdp,
    });
  });

  socket.on("webrtc:candidate", (payload: SignalPayload) => {
    const room = rooms.get(payload.roomId);
    if (!room || !payload.candidate) return;

    const target =
      socket.data.role === "host" ? room.viewerId : room.hostId;

    if (target) {
      io.to(target).emit("webrtc:candidate", {
        from: socket.id,
        roomId: payload.roomId,
        candidate: payload.candidate,
      });
    }
  });

  socket.on("peer:reconnect", (payload: { roomId: string }) => {
    const room = rooms.get(payload.roomId);
    if (!room) return;

    if (socket.data.role === "viewer") {
      io.to(room.hostId).emit("peer:joined", { viewerId: socket.id });
    }
  });

  socket.on("disconnect", () => {
    const roomId = socket.data.roomId as string | undefined;
    const role = socket.data.role as "host" | "viewer" | undefined;
    if (!roomId || !role) return;

    const room = rooms.get(roomId);
    if (!room) return;

    if (role === "host" && room.hostId === socket.id) {
      io.to(roomName(roomId)).emit("room:closed");
      rooms.delete(roomId);
      return;
    }

    if (role === "viewer" && room.viewerId === socket.id) {
      room.viewerId = undefined;
      io.to(room.hostId).emit("peer:left");
    }
  });
});

httpServer.listen(port, () => {
  console.log(`StreamLink signaling server listening on :${port}`);
});
