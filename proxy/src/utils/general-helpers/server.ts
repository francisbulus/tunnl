import { handleRequestError } from "../error-handlers/server.js";
import { ResponseResolver, Req } from "../types.js";

const HOP_BY_HOP_HEADERS = new Set([
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
]);

export const sanitizeHeaders = (
  headers: Record<string, any>
): Record<string, any> => {
  const sanitized: Record<string, any> = {};
  for (const [key, value] of Object.entries(headers || {})) {
    if (HOP_BY_HOP_HEADERS.has(key.toLowerCase())) {
      continue;
    }
    if (typeof value === "undefined") {
      continue;
    }
    sanitized[key] = value;
  }
  return sanitized;
};

export const handleResponse: ResponseResolver = (
  statusCode,
  statusMessage,
  headers,
  inbound,
  res
) => {
  inbound.off("proxy-request-error", handleRequestError);
  res.writeHead(statusCode, statusMessage, sanitizeHeaders(headers));
};

export const getToken = (req: Req) => {
  if (
    req.headers.authorization &&
    req.headers.authorization.split(" ")[0] === "Bearer"
  ) {
    return req.headers.authorization.split(" ")[1];
  } else if (req.query && req.query.token) {
    return req.query.token;
  }
  return null;
};
