const test = require("node:test");
const assert = require("node:assert/strict");
const { io: createSocketClient } = require("socket.io-client");

process.env.ENFORCE_HTTPS = "false";
process.env.RATE_LIMIT_MAX_REQUESTS = "1000";
process.env.REQUEST_TIMEOUT_MS = "10000";
process.env.MAX_REQUEST_BODY_BYTES = "1048576";

const serverModule = require("../build/server.js");
const proxyServer = serverModule.default;

const waitForEvent = (emitter, eventName, timeoutMs = 5000) => {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error(`Timed out waiting for '${eventName}'`));
    }, timeoutMs);

    emitter.once(eventName, (...args) => {
      clearTimeout(timeout);
      resolve(args);
    });
  });
};

const toBuffer = (chunk) => {
  if (Buffer.isBuffer(chunk)) {
    return chunk;
  }
  if (
    chunk &&
    typeof chunk === "object" &&
    chunk.type === "Buffer" &&
    Array.isArray(chunk.data)
  ) {
    return Buffer.from(chunk.data);
  }
  if (typeof chunk === "string") {
    return Buffer.from(chunk);
  }
  return Buffer.from(String(chunk));
};

const normalizeWritevChunk = (chunk) => {
  if (chunk && typeof chunk === "object" && "chunk" in chunk) {
    return toBuffer(chunk.chunk);
  }
  return toBuffer(chunk);
};

const startServer = async () => {
  await new Promise((resolve, reject) => {
    proxyServer.listen(0, "127.0.0.1", (err) => {
      if (err) {
        reject(err);
        return;
      }
      resolve();
    });
  });

  const address = proxyServer.address();
  const port = typeof address === "object" && address ? address.port : null;
  if (!port) {
    throw new Error("Failed to resolve proxy server port");
  }

  return {
    baseUrl: `http://127.0.0.1:${port}`,
    close: async () => {
      await new Promise((resolve, reject) => {
        proxyServer.close((err) => {
          if (err) {
            reject(err);
            return;
          }
          resolve();
        });
      });
    },
  };
};

const createTunnelClient = async (baseUrl, tunnelKey, options = {}) => {
  const socket = createSocketClient(baseUrl, {
    transports: ["websocket"],
    forceNew: true,
    reconnection: false,
  });

  await waitForEvent(socket, "connect");
  socket.emit("join", tunnelKey);
  await waitForEvent(socket, "room-confirmation");

  const receivedRequests = [];

  socket.on("request", (id, req) => {
    const bodyChunks = [];

    const onPipe = (eventId, data) => {
      if (eventId !== id) {
        return;
      }
      bodyChunks.push(toBuffer(data));
    };

    const onPipes = (eventId, data) => {
      if (eventId !== id) {
        return;
      }
      data.forEach((chunk) => {
        bodyChunks.push(normalizeWritevChunk(chunk));
      });
    };

    const cleanupListeners = () => {
      socket.off("inbound-pipe", onPipe);
      socket.off("inbound-pipes", onPipes);
      socket.off("inbound-pipe-end", onPipeEnd);
      socket.off("inbound-pipe-error", onPipeError);
    };

    const onPipeError = (eventId, message) => {
      if (eventId !== id) {
        return;
      }
      cleanupListeners();
      socket.emit("request-error", id, message || "inbound stream failure");
    };

    const onPipeEnd = (eventId) => {
      if (eventId !== id) {
        return;
      }
      cleanupListeners();
      const requestBody = Buffer.concat(bodyChunks).toString("utf8");
      receivedRequests.push({
        id,
        method: req.method,
        path: req.path,
        body: requestBody,
      });

      socket.emit("response", id, {
        statusCode: 200,
        statusMessage: "OK",
        headers: {
          "content-type": "text/plain",
        },
      });
      socket.emit(
        "outbound-pipe",
        id,
        Buffer.from(`${req.method}:${req.path}:${requestBody}`)
      );
      socket.emit("outbound-pipe-end", id);

      if (options.disconnectAfterResponse) {
        setTimeout(() => {
          socket.disconnect();
        }, options.disconnectDelayMs || 25);
      }
    };

    socket.on("inbound-pipe", onPipe);
    socket.on("inbound-pipes", onPipes);
    socket.on("inbound-pipe-end", onPipeEnd);
    socket.on("inbound-pipe-error", onPipeError);
  });

  return {
    socket,
    receivedRequests,
    close: () => {
      socket.disconnect();
    },
  };
};

test("proxy tunnel hardening integration", async (t) => {
  const { baseUrl, close } = await startServer();
  t.after(close);

  await t.test("forwards GET requests for valid tunnel key", async (t) => {
    const tunnel = await createTunnelClient(baseUrl, "GET01");
    t.after(tunnel.close);

    const response = await fetch(`${baseUrl}/hello`, {
      method: "GET",
      headers: {
        Authorization: "Bearer GET01",
      },
    });

    assert.equal(response.status, 200);
    assert.equal(await response.text(), "GET:/hello:");
    assert.equal(tunnel.receivedRequests.length, 1);
    assert.equal(tunnel.receivedRequests[0].method, "GET");
    assert.equal(tunnel.receivedRequests[0].path, "/hello");
  });

  await t.test("forwards POST body for valid tunnel key", async (t) => {
    const tunnel = await createTunnelClient(baseUrl, "POST1");
    t.after(tunnel.close);
    const payload = "forward this body";

    const response = await fetch(`${baseUrl}/submit`, {
      method: "POST",
      headers: {
        Authorization: "Bearer POST1",
        "content-type": "text/plain",
      },
      body: payload,
    });

    assert.equal(response.status, 200);
    assert.equal(await response.text(), `POST:/submit:${payload}`);
    assert.equal(tunnel.receivedRequests.length, 1);
    assert.equal(tunnel.receivedRequests[0].body, payload);
  });

  await t.test(
    "does not append socket error text after successful response",
    async (t) => {
      const tunnel = await createTunnelClient(baseUrl, "DISC2", {
        disconnectAfterResponse: true,
        disconnectDelayMs: 25,
      });
      t.after(tunnel.close);

      const response = await fetch(`${baseUrl}/stable`, {
        method: "GET",
        headers: {
          Authorization: "Bearer DISC2",
        },
      });

      assert.equal(response.status, 200);
      assert.equal(await response.text(), "GET:/stable:");
    }
  );

  await t.test("returns 404 once tunnel disconnects", async (t) => {
    const tunnel = await createTunnelClient(baseUrl, "DISC1");
    const disconnected = waitForEvent(tunnel.socket, "disconnect");
    tunnel.close();
    await disconnected;
    t.after(tunnel.close);

    const response = await fetch(`${baseUrl}/after-disconnect`, {
      headers: {
        Authorization: "Bearer DISC1",
      },
    });

    assert.equal(response.status, 404);
  });
});
