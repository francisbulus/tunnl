import { handleSocketConnectionError } from "../error-handlers/sockets.js";
import { getToken } from "./server.js";
import { Socket } from "../types.js";
import { Request } from "express";
import { Server } from "socket.io";
import { DefaultEventsMap } from "socket.io/dist/typed-events";

type IoServer = Server<DefaultEventsMap, DefaultEventsMap, DefaultEventsMap, any>;

const roomToSocketId = new Map<string, string>();
const socketIdToRoom = new Map<string, string>();

const isHttpsEnforced = (): boolean => {
  if (process.env.ENFORCE_HTTPS) {
    return process.env.ENFORCE_HTTPS === "true";
  }
  return process.env.NODE_ENV === "production";
};

const isValidTunnelKey = (key: string): boolean => {
  return /^[A-Za-z0-9_-]{4,64}$/.test(key);
};

const getTunnelKeyFromHost = (req: Request): string | null => {
  const baseDomain = process.env.TUNNEL_BASE_DOMAIN;
  if (!baseDomain) {
    return null;
  }
  const hostHeader = req.headers.host;
  if (!hostHeader) {
    return null;
  }

  const host = hostHeader.split(":")[0].toLowerCase();
  const normalizedBaseDomain = baseDomain.toLowerCase();
  if (!host.endsWith(`.${normalizedBaseDomain}`)) {
    return null;
  }

  const subdomain = host.slice(0, host.length - normalizedBaseDomain.length - 1);
  if (!subdomain || subdomain.includes(".")) {
    return null;
  }

  return subdomain;
};

export const resolveTunnelKey = (req: Request): string | null => {
  const token = getToken(req);
  if (typeof token === "string" && token.trim().length > 0) {
    return token.trim();
  }
  const subdomainToken = getTunnelKeyFromHost(req);
  if (subdomainToken && subdomainToken.trim().length > 0) {
    return subdomainToken.trim();
  }
  return null;
};

export const registerSocketTunnel = (
  io: IoServer,
  socket: Socket,
  room: string
): { ok: boolean; room?: string; message?: string } => {
  const roomKey = String(room || "").trim();
  if (!isValidTunnelKey(roomKey)) {
    return {
      ok: false,
      message:
        "Invalid tunnel key. Expected 4-64 characters in [A-Za-z0-9_-].",
    };
  }

  const existingSocketId = roomToSocketId.get(roomKey);
  if (existingSocketId && existingSocketId !== socket.id) {
    const existingSocket = io.sockets.sockets.get(existingSocketId);
    if (existingSocket) {
      existingSocket.disconnect(true);
    }
  }

  roomToSocketId.set(roomKey, socket.id);
  socketIdToRoom.set(socket.id, roomKey);
  socket.join(roomKey);

  return { ok: true, room: roomKey };
};

export const cleanupSocketTunnelRegistration = (socketId: string): void => {
  const room = socketIdToRoom.get(socketId);
  if (!room) {
    return;
  }
  socketIdToRoom.delete(socketId);
  if (roomToSocketId.get(room) === socketId) {
    roomToSocketId.delete(room);
  }
};

export const getSocketFromTunnelKey = (
  io: IoServer,
  tunnelKey: string
): Socket | null => {
  const socketId = roomToSocketId.get(tunnelKey);
  if (!socketId) {
    return null;
  }
  const socket = io.sockets.sockets.get(socketId) || null;
  if (!socket || !socket.connected) {
    cleanupSocketTunnelRegistration(socketId);
    return null;
  }
  return socket;
};

export const shouldRejectInsecureRequest = (req: Request): boolean => {
  if (!isHttpsEnforced()) {
    return false;
  }
  const forwardedProtoHeader = req.headers["x-forwarded-proto"];
  const forwardedProto = Array.isArray(forwardedProtoHeader)
    ? forwardedProtoHeader[0]
    : forwardedProtoHeader;
  const normalizedForwardedProto = forwardedProto
    ? forwardedProto.split(",")[0].trim()
    : "";

  return !(req.secure || normalizedForwardedProto === "https");
};

export const shouldRejectInsecureSocket = (socket: Socket): boolean => {
  if (!isHttpsEnforced()) {
    return false;
  }

  const forwardedProtoHeader = socket.handshake.headers["x-forwarded-proto"];
  const forwardedProto = Array.isArray(forwardedProtoHeader)
    ? forwardedProtoHeader[0]
    : forwardedProtoHeader;
  const normalizedForwardedProto = forwardedProto
    ? forwardedProto.split(",")[0].trim()
    : "";

  return !(socket.handshake.secure || normalizedForwardedProto === "https");
};

export const resolveClientIdentifier = (req: Request): string => {
  return req.ip || req.socket.remoteAddress || "unknown";
};

export const handlePing = (msg: string, socket: Socket): void => {
  if (msg !== "ping") return;
  socket.send("pong");
};

export const handleSocketClientDisconnect = (socket: Socket): void => {
  socket.off("error", handleSocketConnectionError);
  socket.off("message", handlePing);
};
