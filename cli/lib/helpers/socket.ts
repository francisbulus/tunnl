import { io, Socket } from "socket.io-client";
import http from "http";
import { nanoid } from "nanoid";
import Inbound from "../streams/inbound";
import Outbound from "../streams/outbound";
import { DefaultEventsMap } from "@socket.io/component-emitter";

let socket: Socket<DefaultEventsMap>;
let heartbeatInterval: NodeJS.Timeout | undefined;

function buildAccessUrl(remoteUrl: string, room: string): string {
  try {
    const parsed = new URL(remoteUrl);
    parsed.pathname = "/";
    parsed.search = `?token=${encodeURIComponent(room)}`;
    return parsed.toString();
  } catch (err) {
    return `/?token=${encodeURIComponent(room)}`;
  }
}

export function persistConnection(): void {
  if (heartbeatInterval) {
    return;
  }
  heartbeatInterval = setInterval(() => {
    if (socket && socket.connected) {
      socket.send("ping");
    }
  }, 5000);
}

export function connect(config: { [option: string]: any }): void {
  const remote = config.remote;
  socket = io(remote, {
    transports: ["websocket"],
    reconnection: true,
    reconnectionAttempts: Infinity,
    timeout: 10000,
  });

  socket.on("connect", () => {
    const room = nanoid(5);
    socket.emit("join", room);
    if (socket.connected) {
      const accessUrl = buildAccessUrl(remote, room);
      console.log(
        `Tunnel key: "${room}"\nAccess with header: Authorization: Bearer ${room}\nOr URL: ${accessUrl}`
      );
    }
  });

  socket.on("connect_error", (e) => {
    console.error(e.message);
  });

  socket.on("room-confirmation", () => {
    console.log("Aye, we are game.");
  });

  socket.on("request", (id, req) => {
    req.hostname = "127.0.0.1";
    req.host = "127.0.0.1";
    req.port = config.port;
    const inbound = new Inbound(id, socket);
    const localServerReq = http.request(req);
    localServerReq.setTimeout(
      Number(process.env.TUNNEL_LOCAL_TIMEOUT_MS || "30000"),
      () => {
        localServerReq.destroy(new Error("Local server request timed out"));
      }
    );
    inbound.pipe(localServerReq);
    const handleLocalServerResponse = (res: {
      statusCode: any;
      statusMessage: any;
      headers: any;
      pipe: (arg0: any) => void;
    }) => {
      localServerReq.off("error", handleLocalServerError);
      const outbound = new Outbound(id, socket);
      outbound.writeHead(res.statusCode, res.statusMessage, res.headers);
      res.pipe(outbound);
    };

    const handleLocalServerError = (err: Error): void => {
      localServerReq.off("response", handleLocalServerResponse);
      socket.emit("request-error", id, err && err.message);
      inbound.destroy(err);
    };
    localServerReq.on("response", handleLocalServerResponse);
    localServerReq.on("error", handleLocalServerError);
  });
  persistConnection();
}
