import { createServer } from "node:http";
import { EventEmitter } from "node:events";
import { spawn } from "node:child_process";
import path from "node:path";
import { Server, type Socket } from "socket.io";

type Room = {
  hostId?: string;
  viewerId?: string;
  createdAt: number;
  file?: {
    name: string;
    size: number;
    mimeType: string;
  };
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
const clientOrigin = process.env.CLIENT_ORIGIN ?? "*";
const rooms = new Map<string, Room>();

const chunkEmitter = new EventEmitter();
chunkEmitter.setMaxListeners(100);

// ───────────── TURN credential configuration ─────────────
function getTurnServers(): Array<{ urls: string | string[]; username?: string; credential?: string }> {
  const turnUrl = process.env.TURN_URL || process.env.NEXT_PUBLIC_TURN_URL;
  if (turnUrl) {
    return [
      {
        urls: turnUrl.split(",").map((url) => url.trim()).filter(Boolean),
        username: process.env.TURN_USERNAME || process.env.NEXT_PUBLIC_TURN_USERNAME || "",
        credential: process.env.TURN_PASSWORD || process.env.NEXT_PUBLIC_TURN_PASSWORD || "",
      },
    ];
  }
  // No external TURN configured — relay will handle cross-network
  return [];
}

// ───────────── FFmpeg path resolution ─────────────
let ffmpegPath = "ffmpeg";
try {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  ffmpegPath = require("ffmpeg-static") as string;
} catch {
  // ffmpeg-static not available, try system ffmpeg
}

/** 
 * Check if a file format needs remuxing for browser playback.
 * Uses an allowlist approach: only formats known to be browser-native are allowed through.
 * Everything else gets transcoded to fragmented MP4 via FFmpeg.
 */
function needsRemux(mimeType: string): boolean {
  const browserNativeTypes = [
    "video/mp4",
    "video/webm",
    "video/ogg",
  ];
  return !browserNativeTypes.includes(mimeType);
}

// ───────────── Chunk request from host ─────────────
function requestChunkFromHost(hostSocketId: string, offset: number, size: number): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const requestId = `req_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;

    const onChunk = (chunk: any) => {
      clearTimeout(timer);
      resolve(Buffer.from(chunk));
    };

    chunkEmitter.once(requestId, onChunk);

    const timer = setTimeout(() => {
      chunkEmitter.off(requestId, onChunk);
      reject(new Error("Timeout waiting for chunk from host"));
    }, 15000);

    // Verify the host socket is still connected before requesting
    const hostSocket = io.sockets.sockets.get(hostSocketId);
    if (!hostSocket || !hostSocket.connected) {
      clearTimeout(timer);
      chunkEmitter.off(requestId, onChunk);
      reject(new Error("Host is not connected"));
      return;
    }

    io.to(hostSocketId).emit("stream:request-chunk", { offset, size, requestId });
  });
}

// ───────────── CORS helper ─────────────
function setCorsHeaders(res: import("node:http").ServerResponse, req: import("node:http").IncomingMessage) {
  const origin = req.headers.origin || "*";
  if (clientOrigin === "*") {
    res.setHeader("Access-Control-Allow-Origin", "*");
  } else {
    const allowed = clientOrigin.split(",").map((o) => o.trim());
    if (allowed.includes(origin)) {
      res.setHeader("Access-Control-Allow-Origin", origin);
    } else {
      // For stream endpoints, allow any origin so cross-network devices can access
      res.setHeader("Access-Control-Allow-Origin", origin);
    }
  }
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Range, Content-Type");
  res.setHeader("Access-Control-Expose-Headers", "Content-Range, Content-Length, Accept-Ranges");
}

// ───────────── HTTP Server ─────────────
const httpServer = createServer((req, res) => {
  const url = req.url || "";

  // ── TURN credentials endpoint ──
  if (url === "/turn-credentials" || url === "/turn-credentials/") {
    setCorsHeaders(res, req);
    if (req.method === "OPTIONS") {
      res.writeHead(200);
      res.end();
      return;
    }
    const turnServers = getTurnServers();
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ iceServers: turnServers }));
    return;
  }

  // ── Health check ──
  if (url === "/health" || url === "/health/") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size }));
    return;
  }

  // ── Stream endpoint ──
  if (url.startsWith("/stream/")) {
    const roomId = url.split("/")[2]?.split("?")[0]?.toUpperCase();
    const room = rooms.get(roomId);

    if (!room || !room.file) {
      setCorsHeaders(res, req);
      res.writeHead(404, { "Content-Type": "text/plain" });
      res.end("Room or file not found.");
      return;
    }

    if (!room.hostId) {
      setCorsHeaders(res, req);
      res.writeHead(503, { "Content-Type": "text/plain" });
      res.end("Host is not connected. Please wait for the host to join.");
      return;
    }

    // Verify host is actually connected
    const hostSocket = io.sockets.sockets.get(room.hostId);
    if (!hostSocket || !hostSocket.connected) {
      setCorsHeaders(res, req);
      res.writeHead(503, { "Content-Type": "text/plain" });
      res.end("Host is not currently online.");
      return;
    }

    const file = room.file;
    const fileSize = file.size;
    const range = req.headers.range;
    const shouldRemux = needsRemux(file.mimeType);

    setCorsHeaders(res, req);

    if (req.method === "OPTIONS") {
      res.writeHead(200);
      res.end();
      return;
    }

    // ── If the format needs remuxing (MKV, AVI, etc.) → pipe through FFmpeg as fragmented MP4 ──
    if (shouldRemux) {
      console.log(`[STREAM REMUX] Transcoding ${file.name} (${file.mimeType}) to fragmented MP4 for browser playback`);

      res.writeHead(200, {
        "Content-Type": "video/mp4",
        "Accept-Ranges": "none",
        "Transfer-Encoding": "chunked",
        "Cache-Control": "no-store",
      });

      // Spawn FFmpeg to transcode to fragmented MP4 (H.264 + AAC)
      // Fragmented MP4 (fMP4) is streamable — no need for moov atom at the end
      // Every browser supports H.264/AAC in MP4 container
      const ffmpegArgs = [
        "-i", "pipe:0",                          // Read from stdin
        "-c:v", "libx264",                       // H.264 video (universally supported)
        "-preset", "ultrafast",                   // Fastest encoding speed
        "-tune", "zerolatency",                   // Low-latency encoding
        "-crf", "23",                             // Good quality
        "-c:a", "aac",                            // AAC audio (universally supported)
        "-b:a", "128k",                           // Audio bitrate
        "-ac", "2",                               // Stereo audio
        "-movflags", "frag_keyframe+empty_moov+default_base_moof",  // Fragmented MP4 for streaming
        "-f", "mp4",                              // Output MP4 format
        "-pix_fmt", "yuv420p",                    // Compatible pixel format
        "pipe:1",                                 // Write to stdout
      ];

      const ffmpeg = spawn(ffmpegPath, ffmpegArgs, {
        stdio: ["pipe", "pipe", "pipe"],
      });

      let ffmpegErrored = false;

      ffmpeg.on("error", (err) => {
        console.error("[STREAM REMUX] FFmpeg spawn error:", err.message);
        ffmpegErrored = true;
        if (!res.writableEnded) res.end();
      });

      ffmpeg.stderr?.on("data", (data: Buffer) => {
        // Log FFmpeg progress but don't treat stderr output as errors
        // FFmpeg writes progress info to stderr normally
        const msg = data.toString().trim();
        if (msg.includes("Error") || msg.includes("error")) {
          console.error("[STREAM REMUX STDERR]", msg);
        }
      });

      ffmpeg.stdout?.on("data", (chunk: Buffer) => {
        if (!res.writableEnded && !ffmpegErrored) {
          const ok = res.write(chunk);
          if (!ok) {
            // Backpressure: wait for drain before continuing
            ffmpeg.stdout?.pause();
            res.once("drain", () => {
              ffmpeg.stdout?.resume();
            });
          }
        }
      });

      ffmpeg.on("close", (code) => {
        console.log(`[STREAM REMUX] FFmpeg process closed with code ${code}`);
        if (!res.writableEnded) res.end();
      });

      // Feed chunks from host to FFmpeg stdin
      let currentOffset = 0;
      const feedChunks = async () => {
        try {
          while (currentOffset < fileSize && !req.destroyed && !ffmpegErrored) {
            const readSize = Math.min(256 * 1024, fileSize - currentOffset);
            const chunk = await requestChunkFromHost(room.hostId || "", currentOffset, readSize);
            if (ffmpeg.stdin?.writable && !ffmpegErrored) {
              const canWrite = ffmpeg.stdin.write(chunk);
              if (!canWrite) {
                // Wait for FFmpeg stdin to drain before writing more
                await new Promise<void>((resolve) => ffmpeg.stdin!.once("drain", resolve));
              }
            } else {
              break;
            }
            currentOffset += readSize;
          }
          if (ffmpeg.stdin?.writable) ffmpeg.stdin.end();
        } catch (err) {
          console.error(`[STREAM REMUX ERROR]`, err);
          if (ffmpeg.stdin?.writable) ffmpeg.stdin.end();
          if (!res.writableEnded) res.end();
        }
      };

      void feedChunks();

      req.on("close", () => {
        if (!ffmpeg.killed) ffmpeg.kill("SIGTERM");
      });
      return;
    }

    // ── Standard streaming (browser-playable formats like MP4, WebM) ──
    if (range) {
      const parts = range.replace(/bytes=/, "").split("-");
      const start = parseInt(parts[0], 10);
      const end = parts[1] ? parseInt(parts[1], 10) : fileSize - 1;

      if (start >= fileSize || end >= fileSize) {
        res.writeHead(416, {
          "Content-Range": `bytes */${fileSize}`,
        });
        res.end();
        return;
      }

      const chunksize = end - start + 1;
      res.writeHead(206, {
        "Content-Range": `bytes ${start}-${end}/${fileSize}`,
        "Accept-Ranges": "bytes",
        "Content-Length": chunksize,
        "Content-Type": file.mimeType || "video/mp4",
      });

      let currentOffset = start;
      const streamChunks = async () => {
        try {
          while (currentOffset <= end && !req.destroyed) {
            const readSize = Math.min(256 * 1024, end - currentOffset + 1);
            const chunk = await requestChunkFromHost(room.hostId || "", currentOffset, readSize);
            if (!res.writableEnded) res.write(chunk);
            currentOffset += readSize;
          }
          if (!res.writableEnded) res.end();
        } catch (err) {
          console.error(`[STREAM ERROR] Error streaming range to client:`, err);
          if (!res.writableEnded) res.end();
        }
      };
      void streamChunks();
    } else {
      res.writeHead(200, {
        "Content-Length": fileSize,
        "Content-Type": file.mimeType || "video/mp4",
        "Accept-Ranges": "bytes",
      });

      let currentOffset = 0;
      const streamAll = async () => {
        try {
          while (currentOffset < fileSize && !req.destroyed) {
            const readSize = Math.min(256 * 1024, fileSize - currentOffset);
            const chunk = await requestChunkFromHost(room.hostId || "", currentOffset, readSize);
            if (!res.writableEnded) res.write(chunk);
            currentOffset += readSize;
          }
          if (!res.writableEnded) res.end();
        } catch (err) {
          console.error(`[STREAM ERROR] Error streaming full file to client:`, err);
          if (!res.writableEnded) res.end();
        }
      };
      void streamAll();
    }
    return;
  }

  if (!url.startsWith("/socket.io/")) {
    res.writeHead(404, { "Content-Type": "text/plain" });
    res.end("Not Found");
  }
});

// ───────────── Socket.IO Server ─────────────
const io = new Server(httpServer, {
  cors: {
    origin: clientOrigin === "*" ? "*" : clientOrigin.split(",").map((origin) => origin.trim()),
    methods: ["GET", "POST"],
    credentials: clientOrigin !== "*",
  },
  transports: ["websocket", "polling"],
  maxHttpBufferSize: 1e7, // 10MB max for relay chunks
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
    rooms.set(roomId, { hostId: "", viewerId: socket.id, createdAt: Date.now() });
    bindSocket(socket, roomId, "viewer");
    console.log(`[ROOM CREATE] Viewer (Creator) ${socket.id} created room ${roomId}`);
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

      const room = rooms.get(roomId);

      if (payload.role === "viewer") {
        rooms.set(roomId, { hostId: room?.hostId || "", viewerId: socket.id, createdAt: Date.now(), file: room?.file });
        bindSocket(socket, roomId, "viewer");
        console.log(`[ROOM RESTORE SUCCESS] Viewer ${socket.id} restored room ${roomId}`);
        ack({ ok: true });
        return;
      }

      if (!room) {
        console.log(`[ROOM RESTORE FAIL] Room ${roomId} not found for host ${socket.id}`);
        ack({ ok: false, error: "Room is no longer available." });
        return;
      }

      // FIX: Allow restore if the room has no active host, OR the previous host is disconnected
      // (socket IDs change on reconnect, so comparing to old socket.id is unreliable)
      if (room.hostId) {
        const existingHost = io.sockets.sockets.get(room.hostId);
        if (existingHost && existingHost.connected && existingHost.id !== socket.id) {
          console.log(`[ROOM RESTORE FAIL] Room ${roomId} already has an active host (${room.hostId})`);
          ack({ ok: false, error: "This room already has a host." });
          return;
        }
      }

      room.hostId = socket.id;
      bindSocket(socket, roomId, "host");
      console.log(`[ROOM RESTORE SUCCESS] Host ${socket.id} restored connection in room ${roomId}`);
      if (room.viewerId) {
        io.to(room.viewerId).emit("peer:joined", { hostId: socket.id });
        socket.emit("peer:joined", { viewerId: room.viewerId });
      }
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
      console.log(`[ROOM JOIN REQUEST] Host ${socket.id} requesting to join room ${roomId}`);

      if (!room) {
        console.log(`[ROOM JOIN FAIL] Room ${roomId} not found for host ${socket.id}`);
        ack({ ok: false, error: "Room not found. Check the invite code." });
        return;
      }

      // FIX: Check if existing host is actually still connected
      if (room.hostId && room.hostId !== socket.id) {
        const existingHost = io.sockets.sockets.get(room.hostId);
        if (existingHost && existingHost.connected) {
          console.log(`[ROOM JOIN FAIL] Room ${roomId} already occupied by active host ${room.hostId}`);
          ack({ ok: false, error: "This room already has a host." });
          return;
        }
        // Previous host disconnected — allow new host
        console.log(`[ROOM JOIN] Previous host ${room.hostId} is disconnected, allowing ${socket.id}`);
      }

      room.hostId = socket.id;
      bindSocket(socket, roomId, "host");
      console.log(`[ROOM JOIN SUCCESS] Host ${socket.id} joined room ${roomId}`);
      
      if (room.viewerId) {
        io.to(room.viewerId).emit("peer:joined", { hostId: socket.id });
        socket.emit("peer:joined", { viewerId: room.viewerId });
      }
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
    if (room.hostId) {
      io.to(room.hostId).emit("webrtc:answer", {
        from: socket.id,
        roomId: payload.roomId,
        sdp: payload.sdp,
      });
    }
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
    if (socket.data.role === "viewer" && room.hostId) {
      io.to(room.hostId).emit("peer:joined", { viewerId: socket.id });
    } else if (socket.data.role === "host" && room.viewerId) {
      io.to(room.viewerId).emit("peer:joined", { hostId: socket.id });
    }
  });

  socket.on(
    "room:register-file",
    (payload: { roomId: string; file: { name: string; size: number; mimeType: string } }) => {
      const roomId = payload.roomId.toUpperCase();
      const room = rooms.get(roomId);
      if (room && room.hostId === socket.id) {
        room.file = payload.file;
        console.log(`[FILE REGISTER] Room ${roomId} registered file: ${payload.file.name} (${payload.file.size} bytes, ${payload.file.mimeType})`);
        // Notify viewer about the file
        if (room.viewerId) {
          io.to(room.viewerId).emit("host:file-registered" as any, {
            name: payload.file.name,
            size: payload.file.size,
            mimeType: payload.file.mimeType,
          });
        }
      }
    }
  );

  socket.on("stream:respond-chunk", (payload: { requestId: string; chunk: any }) => {
    chunkEmitter.emit(payload.requestId, payload.chunk);
  });

  // ── WebSocket relay for when WebRTC fails ──
  socket.on("relay:data", (payload: { roomId: string; data: ArrayBuffer | string }) => {
    const room = rooms.get(payload.roomId);
    if (!room) return;

    const target = socket.data.role === "host" ? room.viewerId : room.hostId;
    if (target) {
      io.to(target).emit("relay:data", { data: payload.data });
    }
  });

  socket.on("disconnect", () => {
    const roomId = socket.data.roomId as string | undefined;
    const role = socket.data.role as "host" | "viewer" | undefined;
    console.log(`[SOCKET DISCONNECT] Socket ${socket.id} disconnected. Role: ${role || "none"}, Room ID: ${roomId || "none"}`);
    if (!roomId || !role) return;

    const room = rooms.get(roomId);
    if (!room) return;

    if (role === "viewer" && room.viewerId === socket.id) {
      console.log(`[ROOM CLOSE] Viewer (Creator) disconnected. Closing room ${roomId}`);
      io.to(roomName(roomId)).emit("room:closed");
      rooms.delete(roomId);
      return;
    }

    if (role === "host" && room.hostId === socket.id) {
      console.log(`[ROOM LEAVE] Host disconnected from room ${roomId}`);
      room.hostId = undefined;
      if (room.viewerId) {
        io.to(room.viewerId).emit("peer:left");
      }
    }
  });
});

// ───────────── Room cleanup interval ─────────────
setInterval(() => {
  const now = Date.now();
  const maxAge = 2 * 60 * 60 * 1000; // 2 hours
  for (const [id, room] of rooms) {
    if (now - room.createdAt > maxAge) {
      console.log(`[ROOM EXPIRE] Cleaning up stale room ${id}`);
      io.to(roomName(id)).emit("room:closed");
      rooms.delete(id);
    }
  }
}, 10 * 60 * 1000); // Check every 10 minutes

httpServer.listen(port, () => {
  console.log(`StreamLink signaling server listening on :${port}`);
});

// --
