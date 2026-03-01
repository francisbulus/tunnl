import express, { Express } from "express";
import http from "http";
import { Server } from "socket.io";
import morgan from "morgan";
import {
  handleBadRequestToSocket,
  handleRequestError,
  handleSocketError,
} from "./utils/error-handlers/server.js";
import {
  cleanupSocketTunnelRegistration,
  getSocketFromTunnelKey,
  handleSocketClientDisconnect,
  handlePing,
  registerSocketTunnel,
  resolveClientIdentifier,
  resolveTunnelKey,
  shouldRejectInsecureRequest,
  shouldRejectInsecureSocket,
} from "./utils/general-helpers/sockets.js";
import { handleResponse, sanitizeHeaders } from "./utils/general-helpers/server.js";
import { handleSocketConnectionError } from "./utils/error-handlers/sockets.js";
import cors from "cors";
import crypto from "crypto";
import Inbound from "./streams/inbound.js";
import Outbound from "./streams/outbound.js";
import { Req } from "./utils/types.js";
import { DefaultEventsMap } from "socket.io/dist/typed-events";
import path from "path";

const app: Express = express();
const server = http.createServer(app);
const io: Server<DefaultEventsMap, DefaultEventsMap, DefaultEventsMap, any> =
  new Server(server);

const REQUEST_TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS || "30000");
const RATE_LIMIT_WINDOW_MS = Number(process.env.RATE_LIMIT_WINDOW_MS || "60000");
const RATE_LIMIT_MAX_REQUESTS = Number(
  process.env.RATE_LIMIT_MAX_REQUESTS || "120"
);
const MAX_REQUEST_BODY_BYTES = Number(
  process.env.MAX_REQUEST_BODY_BYTES || `${10 * 1024 * 1024}`
);

type RateBucket = {
  count: number;
  resetAt: number;
};

const rateBuckets = new Map<string, RateBucket>();

const isRateLimited = (clientId: string): boolean => {
  const now = Date.now();
  const existingBucket = rateBuckets.get(clientId);
  if (!existingBucket || existingBucket.resetAt <= now) {
    rateBuckets.set(clientId, {
      count: 1,
      resetAt: now + RATE_LIMIT_WINDOW_MS,
    });
    return false;
  }

  existingBucket.count += 1;
  return existingBucket.count > RATE_LIMIT_MAX_REQUESTS;
};

setInterval(() => {
  const now = Date.now();
  for (const [clientId, bucket] of rateBuckets.entries()) {
    if (bucket.resetAt <= now) {
      rateBuckets.delete(clientId);
    }
  }
}, RATE_LIMIT_WINDOW_MS).unref();

server.requestTimeout = REQUEST_TIMEOUT_MS;
server.headersTimeout = REQUEST_TIMEOUT_MS + 5000;

io.on("connection", (socket): void => {
  if (shouldRejectInsecureSocket(socket)) {
    socket.emit("join-error", {
      message: "Secure websocket connection is required",
    });
    socket.disconnect(true);
    return;
  }

  socket.once("join", function (room): void {
    const result = registerSocketTunnel(io, socket, room);
    if (!result.ok) {
      socket.emit("join-error", {
        message: result.message,
      });
      socket.disconnect(true);
      return;
    }
    socket.emit("room-confirmation", {
      message: `You've been connected!`,
      room: result.room,
      joined_at: Date.now(),
    });
  });

  socket.on("message", function (msg): void {
    handlePing(msg, socket);
  });

  socket.once("disconnect", function (): void {
    cleanupSocketTunnelRegistration(socket.id);
    handleSocketClientDisconnect(socket);
  });

  socket.once("error", function (): void {
    cleanupSocketTunnelRegistration(socket.id);
    handleSocketConnectionError(socket);
  });
});

app.set("trust proxy", true);
app.disable("x-powered-by");
app.use(morgan("tiny"));
app.use(cors());
app.get("/healthz", (_req, res) => {
  res.status(200).json({ status: "ok" });
});

app.get("/connect", (_req, res) => {
  res.sendFile("index.html", {
    root: path.resolve(process.cwd(), "src/public"),
  });
});

app.use(
  "/",
  (req, res) => {
    if (shouldRejectInsecureRequest(req)) {
      res.status(426).send("HTTPS is required");
      return;
    }

    if (isRateLimited(resolveClientIdentifier(req))) {
      res.status(429).send("Rate limit exceeded");
      return;
    }

    const tunnelKey = resolveTunnelKey(req);
    if (!tunnelKey) {
      res.status(401).send("Tunnel key required");
      return;
    }

    const socket = getSocketFromTunnelKey(io, tunnelKey);
    if (!socket) {
      res.status(404).send("No active tunnel for the provided key");
      return;
    }

    const contentLengthHeader = req.headers["content-length"];
    const contentLengthValue = Array.isArray(contentLengthHeader)
      ? contentLengthHeader[0]
      : contentLengthHeader;
    if (contentLengthValue) {
      const parsedContentLength = Number(contentLengthValue);
      if (
        Number.isFinite(parsedContentLength) &&
        parsedContentLength > MAX_REQUEST_BODY_BYTES
      ) {
        res.status(413).send("Payload too large");
        return;
      }
    }

    const id = crypto.randomUUID();
    const inbound = new Inbound(id, socket, {
      method: req.method,
      headers: sanitizeHeaders(Object.assign({}, req.headers)),
      path: req.url,
    });

    req.setTimeout(REQUEST_TIMEOUT_MS, () => {
      inbound.destroy(new Error("Client request timed out"));
      if (!res.headersSent) {
        res.status(408).end("Request timeout");
      }
    });

    let bytesReceived = 0;
    const onData = (chunk: Buffer) => {
      bytesReceived += chunk.length;
      if (bytesReceived <= MAX_REQUEST_BODY_BYTES) {
        return;
      }

      req.off("data", onData);
      req.unpipe(inbound);
      inbound.destroy(new Error("Payload too large"));
      if (!res.headersSent) {
        res.status(413).end("Payload too large");
      }
      req.destroy(new Error("Payload too large"));
    };
    req.on("data", onData);

    req.once("aborted", function () {
      handleBadRequestToSocket("Request aborted by client", req as Req);
    });
    req.once("error", function (err: Error) {
      handleBadRequestToSocket(err, req as Req);
    });
    req.once("close", () => {
      req.off("data", onData);
    });

    req.pipe(inbound);
    const outbound = new Outbound(id, socket);

    const timeout = setTimeout(() => {
      outbound.destroy(new Error("Tunnel response timed out"));
      if (!res.headersSent) {
        res.status(504).end("Gateway timeout");
      }
    }, REQUEST_TIMEOUT_MS);

    const clearResponseTimeout = () => {
      clearTimeout(timeout);
    };

    const handleSocketErrorWrapper = () => {
      clearResponseTimeout();
      handleSocketError(res, socket);
    };

    outbound.once("proxy-request-error", function (errorMessage?: string) {
      clearResponseTimeout();
      handleRequestError(res, outbound, errorMessage);
    });
    outbound.once("response", function (statusCode, statusMessage, headers) {
      clearResponseTimeout();
      handleResponse(statusCode, statusMessage, headers, inbound, res);
    });
    outbound.once("error", handleSocketErrorWrapper);

    outbound.pipe(res);
    res.once("close", () => {
      clearResponseTimeout();
      req.off("data", onData);
      socket.off("close", handleSocketErrorWrapper);
      outbound.off("error", handleSocketErrorWrapper);
    });
    socket.once("close", handleSocketErrorWrapper);
  }
);

export { io };

export default server;
