import arg from "arg";
import { ComputedArguments } from "../types";

export function parseArgumentsIntoOptions(
  rawArgs: string[]
): ComputedArguments {
  const args = arg(
    {
      "-p": "--port",
      "-r": "--remote",
      "--help": Boolean,
      "--version": Boolean,
      "--port": Number,
      "--remote": String,
    },
    {
      argv: rawArgs.slice(2),
    }
  );

  if (args["--help"]) {
    console.log(
      "Usage: tunnel -p <local-port> [--remote <proxy-url>]\n\n" +
        "Options:\n" +
        "  -p, --port    Local port to expose\n" +
        "  -r, --remote  Remote proxy URL (defaults to TUNNEL_REMOTE_URL or http://localhost:1337)\n" +
        "      --help    Show help\n" +
        "      --version Print version"
    );
    process.exit(0);
  }

  if (args["--version"]) {
    console.log("1.0.0");
    process.exit(0);
  }

  if (!args["--port"]) {
    console.error("-p flag not provided");
    process.exit(1);
  }

  return {
    port: args["--port"] || false,
    remote: args["--remote"],
  };
}
