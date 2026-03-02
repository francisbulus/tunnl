import { Readable } from "stream";
import { Socket, StreamHandler } from "../types";

const normalizeChunk = (chunk: any): Buffer | string => {
  if (Buffer.isBuffer(chunk) || typeof chunk === "string") {
    return chunk;
  }

  if (chunk && typeof chunk === "object" && "chunk" in chunk) {
    return normalizeChunk(chunk.chunk);
  }

  if (
    chunk &&
    typeof chunk === "object" &&
    chunk.type === "Buffer" &&
    Array.isArray(chunk.data)
  ) {
    return Buffer.from(chunk.data);
  }

  if (chunk instanceof Uint8Array) {
    return Buffer.from(chunk);
  }

  return Buffer.from(String(chunk));
};

export default class Inbound extends Readable {
  constructor(public id: any, public socket: Socket) {
    super();
    this.socket = socket;
    this.id = id;

    const handlePipe: StreamHandler = (id, data) => {
      if (this.id === id) this.push(normalizeChunk(data));
    };

    const handlePipes: StreamHandler = (id, data) => {
      if (this.id === id)
        data.forEach((chunk: any) => this.push(normalizeChunk(chunk)));
    };

    const handlePipeError: StreamHandler = (id, err) => {
      if (this.id === id) {
        this.socket.off("inbound-pipe", handlePipe);
        this.socket.off("inbound-pipes", handlePipes);
        this.socket.off("inbound-pipe-error", handlePipeError);
        this.socket.off("inbound-pipe-end", handlePipeEnd);
        this.destroy(new Error(err));
      }
    };

    const handlePipeEnd: StreamHandler = (id, data) => {
      if (this.id === id) {
        this.socket.off("inbound-pipe", handlePipe);
        this.socket.off("inbound-pipes", handlePipes);
        this.socket.off("inbound-pipe-error", handlePipeError);
        this.socket.off("inbound-pipe-end", handlePipeEnd);
      }
      if (data) {
        this.push(normalizeChunk(data));
      } else {
        this.push(null);
      }
    };

    this.socket.on("inbound-pipe", handlePipe);
    this.socket.on("inbound-pipes", handlePipes);
    this.socket.on("inbound-pipe-error", handlePipeError);
    this.socket.on("inbound-pipe-end", handlePipeEnd);
  }
  _read() {}
}
