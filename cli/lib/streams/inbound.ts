import { Readable } from "stream";
import { Socket, StreamHandler } from "../types";

export default class Inbound extends Readable {
  constructor(public id: any, public socket: Socket) {
    super();
    this.socket = socket;
    this.id = id;

    const handlePipe: StreamHandler = (id, data) => {
      if (this.id === id) this.push(data);
    };

    const handlePipes: StreamHandler = (id, data) => {
      if (this.id === id) data.forEach((chunk: any) => this.push(chunk));
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
        this.push(data);
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
