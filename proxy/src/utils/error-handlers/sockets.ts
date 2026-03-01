import {
  handlePing,
  handleSocketClientDisconnect,
} from "../general-helpers/sockets.js";
import { Socket } from "../types";

export const handleSocketConnectionError = async (
  socket: Socket
) => {
  socket.off("message", handlePing);
  socket.off("disconnect", handleSocketClientDisconnect);
};
