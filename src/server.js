import express from "express";
import http from "http";
import { Server } from "socket.io";
import Request from "./streams/request.js";
import Response from "./streams/response.js";
import morgan from "morgan";
import crypto from "crypto";
import {
  handleBadRequestToSocket,
  handleRequestError,
  handleSocketError,
} from "./utils/error-handlers/server.js";
import {
  handleSocketClientDisconnect,
  handlePing,
} from "./utils/general-helpers/sockets.js";
import { handleResponse } from "./utils/general-helpers/server.js";
import { handleSocketConnectionError } from "./utils/error-handlers/sockets.js";
import { checkConnection } from "./utils/general-helpers/sockets.js";

const app = express();
const server = http.createServer(app);
const io = new Server(server);
let connections = {};

io.on("connection", (socket) => {
  const host = socket.handshake.headers.host;
  connections[host] = socket;
  socket.on("message", handlePing.bind(null, socket));
  socket.once("disconnect", function () {
    handleSocketClientDisconnect(socket, connections);
  });
  socket.once("error", function () {
    handleSocketConnectionError(socket, connections);
  });
});

app.use(morgan("tiny"));
app.use(
  "/",
  (req, res, next) => {
    checkConnection(req, res, next, connections);
  },
  (req, res) => {
    const socket = res.locals.socket;
    const id = crypto.randomUUID();
    const inbound = new Request({
      id,
      socket,
      req: {
        method: req.method,
        headers: Object.assign({}, req.headers),
        path: req.url,
      },
    });

    req.once("aborted", handleBadRequestToSocket.bind(null, req));
    req.once("error", handleBadRequestToSocket.bind(null, req));
    req.once("finish", () => {
      req.off("aborted", handleBadRequestToSocket.bind(null, req));
      req.off("error", handleBadRequestToSocket.bind(null, req));
    });
    req.pipe(inbound);
    const outbound = new Response({ id, socket });

    const handleSocketErrorWrapper = () => {
      handleSocketError(res);
    };

    outbound.once("requestError", function () {
      handleRequestError(res, outbound);
    });
    outbound.once("response", function (statusCode, statusMessage, headers) {
      handleResponse(statusCode, statusMessage, headers, inbound, res);
    });
    outbound.once("error", handleSocketErrorWrapper);
    outbound.pipe(res);
    res.once("close", () => {
      socket.off("close", handleSocketErrorWrapper);
      outbound.off("error", handleSocketErrorWrapper);
    });
    socket.once("close", handleSocketErrorWrapper);
  }
);

export default server;