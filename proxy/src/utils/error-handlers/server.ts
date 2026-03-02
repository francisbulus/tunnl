import { handleResponse } from "../general-helpers/server";
import { Res, Req, Socket } from "../types";

export const handleBadRequestToSocket = (
  err: string | Error,
  request: Req
): void => {
  if (typeof err === "string") {
    request.destroy(new Error(err));
    return;
  }
  request.destroy(err);
};

export const handleSocketError = (res: Res, _socket: Socket): void => {
  if (res.writableEnded || res.destroyed) {
    return;
  }
  if (!res.headersSent) {
    res.status(502);
  }
  res.end("Tunnel socket error");
};

export const handleRequestError = (
  res: Res,
  outbound: any,
  errorMessage?: string
): void => {
  outbound.off("response", handleResponse);
  outbound.destroy();
  if (!res.headersSent) {
    res.status(502);
  }
  res.end(errorMessage || "Upstream request failed");
};
