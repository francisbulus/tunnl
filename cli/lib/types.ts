import { Socket as Client } from "socket.io-client";
import { DefaultEventsMap } from "@socket.io/component-emitter";

export type Socket = Client<DefaultEventsMap>;

export interface StreamHandler {
  (id: number, data: any): void;
}

export type StreamCallback = (error?: Error) => void;

export interface ComputedArguments {
  port: number | false;
  remote?: string;
}
