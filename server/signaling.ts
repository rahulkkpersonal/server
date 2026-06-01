import { createServer } from "node:http";
import { EventEmitter } from "node:events";
import { spawn } from "node:child_process";
import ffmpegPath from "ffmpeg-static";
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
const clientOrigin = process.env.CLIENT_ORIGIN ?? "http://localhost:3000";
const rooms = new Map<string, Room>();

const chunkEmitter = new EventEmitter();
chunkEmitter.setMaxListeners(100);

function requestChunkFromHost(
  hostSocketId: string,
  offset: number,
  size: number,
  req: any
): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    if (req.destroyed) {
      return reject(new Error("Request already destroyed"));
    }

    const requestId = `req_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;

    const onChunk = (chunk: any) => {
      req.off("close", onClose);
      clearTimeout(timer);
      resolve(Buffer.from(chunk));
    };

    const onClose = () => {
      chunkEmitter.off(requestId, onChunk);
      clearTimeout(timer);
      reject(new Error("Client connection closed"));
    };

    chunkEmitter.once(requestId, onChunk);
    req.once("close", onClose);

    const timer = setTimeout(() => {
      chunkEmitter.off(requestId, onChunk);
      req.off("close", onClose);
      reject(new Error("Timeout waiting for chunk from host"));
    }, 15000);

    io.to(hostSocketId).emit("stream:request-chunk", { offset, size, requestId });
  });
}

const httpServer = createServer((req, res) => {
  const url = req.url || "";

  if (url.startsWith("/stream/")) {
    const roomId = url.split("/")[2]?.toUpperCase();
    const room = rooms.get(roomId);

    if (!room || !room.file) {
      res.writeHead(404, { "Content-Type": "text/plain", "Access-Control-Allow-Origin": "*" });
      res.end("Room or file not found.");
      return;
    }

    const file = room.file;
    const fileSize = file.size;
    const range = req.headers.range;

    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Range");

    if (req.method === "OPTIONS") {
      res.writeHead(200);
      res.end();
      return;
    }

    const nonNativeContainers = [".mkv", ".avi", ".flv", ".wmv", ".ts", ".mov", ".mpg", ".mpeg", ".hevc"];
    const isNonNative = file.name && nonNativeContainers.some(ext => file.name.toLowerCase().endsWith(ext));

    if (isNonNative) {
      if (!ffmpegPath) {
        console.error("[TRANSCODE ERROR] ffmpeg-static path is not found.");
        res.writeHead(500, { "Content-Type": "text/plain", "Access-Control-Allow-Origin": "*" });
        res.end("Transcoding service not available.");
        return;
      }

      res.writeHead(200, {
        "Content-Type": "video/mp4",
        "Cache-Control": "no-store",
        "X-Content-Type-Options": "nosniff",
      });

      console.log(`[TRANSCODE START] Spawning FFmpeg to remux/transcode non-native file: ${file.name}`);

      const ffmpeg = spawn(ffmpegPath, [
        "-i", "pipe:0", // Read input from stdin
        "-c:v", "copy", // Copy video track (highly efficient!)
        "-c:a", "aac",  // Transcode audio to AAC (highly compatible)
        "-b:a", "128k", // Audio bitrate
        "-ac", "2",     // Stereo audio
        "-f", "mp4",    // Fragmented MP4 output format
        "-movflags", "empty_moov+omit_tfhd_offset+frag_keyframe+default_base_moof", // Streamable MP4 flags
        "pipe:1"        // Write output to stdout
      ]);

      ffmpeg.stdout.pipe(res);

      ffmpeg.on("error", (err) => {
        console.error("[FFMPEG PROCESS ERROR]", err);
        if (!res.headersSent) {
          res.writeHead(500);
        }
        res.end();
      });

      let currentOffset = 0;
      let isFeederRunning = true;

      const feedFfmpeg = async () => {
        try {
          while (currentOffset < fileSize && !req.destroyed && isFeederRunning) {
            if (!ffmpeg.stdin.writable) {
              console.log("[TRANSCODE FEED] FFmpeg stdin is no longer writable.");
              break;
            }

            const readSize = Math.min(256 * 1024, fileSize - currentOffset);
            const chunk = await requestChunkFromHost(room.hostId || "", currentOffset, readSize, req);

            const canWrite = ffmpeg.stdin.write(chunk);
            if (!canWrite) {
              await new Promise<void>((resolve) => {
                ffmpeg.stdin.once("drain", resolve);
              });
            }

            currentOffset += readSize;
          }
          if (ffmpeg.stdin.writable) {
            ffmpeg.stdin.end();
          }
        } catch (err) {
          console.error("[TRANSCODE FEED ERROR]", err);
          isFeederRunning = false;
          ffmpeg.kill();
          res.end();
        }
      };

      void feedFfmpeg();

      req.on("close", () => {
        console.log("[TRANSCODE END] Client closed connection. Killing FFmpeg process.");
        isFeederRunning = false;
        ffmpeg.kill();
      });

      return;
    }

    if (range) {
      const parts = range.replace(/bytes=/, "").split("-");
      const start = parseInt(parts[0], 10);
      const end = parts[1] ? parseInt(parts[1], 10) : fileSize - 1;

      if (start >= fileSize || end >= fileSize) {
        res.writeHead(416, {
          "Content-Range": `bytes */${fileSize}`,
          "Access-Control-Allow-Origin": "*"
        });
        res.end();
        return;
      }

      const chunksize = end - start + 1;
      res.writeHead(206, {
        "Content-Range": `bytes ${start}-${end}/${fileSize}`,
        "Accept-Ranges": "bytes",
        "Content-Length": chunksize,
        "Content-Type": file.mimeType,
      });

      let currentOffset = start;
      const streamChunks = async () => {
        try {
          while (currentOffset <= end && !req.destroyed) {
            const readSize = Math.min(256 * 1024, end - currentOffset + 1); // 256KB chunks
            const chunk = await requestChunkFromHost(room.hostId || "", currentOffset, readSize, req);
            res.write(chunk);
            currentOffset += readSize;
          }
          res.end();
        } catch (err) {
          console.error(`[STREAM ERROR] Error streaming range to client:`, err);
          if (!res.headersSent) {
            res.writeHead(500);
          }
          res.end();
        }
      };
      void streamChunks();
    } else {
      res.writeHead(200, {
        "Content-Length": fileSize,
        "Content-Type": file.mimeType,
        "Accept-Ranges": "bytes",
      });

      let currentOffset = 0;
      const streamAll = async () => {
        try {
          while (currentOffset < fileSize && !req.destroyed) {
            const readSize = Math.min(256 * 1024, fileSize - currentOffset);
            const chunk = await requestChunkFromHost(room.hostId || "", currentOffset, readSize, req);
            res.write(chunk);
            currentOffset += readSize;
          }
          res.end();
        } catch (err) {
          console.error(`[STREAM ERROR] Error streaming full file to client:`, err);
          if (!res.headersSent) {
            res.writeHead(500);
          }
          res.end();
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

      if (room.hostId && room.hostId !== socket.id) {
        console.log(`[ROOM RESTORE FAIL] Room ${roomId} already has another host (${room.hostId}) instead of ${socket.id}`);
        ack({ ok: false, error: "This room already has a host." });
        return;
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

      if (room.hostId && room.hostId !== socket.id) {
        console.log(`[ROOM JOIN FAIL] Room ${roomId} already occupied by host ${room.hostId} (requested by ${socket.id})`);
        ack({ ok: false, error: "This room already has a host." });
        return;
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
        console.log(`[FILE REGISTER] Room ${roomId} registered file: ${payload.file.name} (${payload.file.size} bytes)`);
      }
    }
  );

  socket.on("stream:respond-chunk", (payload: { requestId: string; chunk: any }) => {
    chunkEmitter.emit(payload.requestId, payload.chunk);
  });

  socket.on("disconnect", () => {
    const roomId = socket.data.roomId as string | undefined;
    const role = socket.data.role as "host" | "viewer" | undefined;
    console.log(`[SOCKET DISCONNECT] Socket ${socket.id} disconnected. Role: ${role || "none"}, Room ID: ${roomId || "none"}`);
    if (!roomId || !role) return;

    const room = rooms.get(roomId);
    if (!room) return;

    if (role === "viewer" && room.viewerId === socket.id) {
      room.viewerId = undefined;
      console.log(`[ROOM LEAVE] Viewer disconnected from room ${roomId}`);
      if (room.hostId) {
        io.to(room.hostId).emit("peer:left");
      } else {
        console.log(`[ROOM CLOSE] No peers left. Deleting room ${roomId}`);
        rooms.delete(roomId);
      }
      return;
    }

    if (role === "host" && room.hostId === socket.id) {
      room.hostId = undefined;
      console.log(`[ROOM LEAVE] Host disconnected from room ${roomId}`);
      if (room.viewerId) {
        io.to(room.viewerId).emit("peer:left");
      } else {
        console.log(`[ROOM CLOSE] No peers left. Deleting room ${roomId}`);
        rooms.delete(roomId);
      }
    }
  });
});

httpServer.listen(port, () => {
  console.log(`StreamLink signaling server listening on :${port}`);
});

// --
