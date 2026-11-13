import { it, expect, vi, beforeEach, afterEach } from "vitest";
import { ReadableStream, WritableStream } from "node:stream/web";
const agents = vi.hoisted(() => []);
vi.mock("@/protocol-agent-factory", () => ({
  createProtocolAgent: () => {
    const a = {
      processStatus: vi.fn(),
      processFrame: vi.fn(),
      handleMessage: vi.fn(),
    };
    agents.push(a);
    return a;
  },
}));
const tick = async () => {
  for (let i = 0; i < 30; i++) await Promise.resolve();
};
let transports, post, websockets;
beforeEach(async () => {
  vi.resetModules();
  agents.length = 0;
  transports = [];
  websockets = [];
  vi.stubGlobal(
    "WebTransport",
    class {
      constructor() {
        this.ready = new Promise((resolve, reject) => {
          this.resolve = resolve;
          this.reject = reject;
        });
        this.closed = new Promise(() => {});
        this.close = vi.fn();
        this.writes = [];
        this.createBidirectionalStream = vi.fn(async () => ({
          readable: new ReadableStream({ start: (c) => (this.controller = c) }),
          writable: new WritableStream({
            write: (bytes) => this.writes.push(bytes),
          }),
        }));
        transports.push(this);
      }
    },
  );
  vi.stubGlobal(
    "WebSocket",
    class {
      constructor() {
        websockets.push(this);
      }
      close() {}
    },
  );
  self.postMessage = vi.fn();
  self.close = vi.fn();
  await import("@/transport/web-socket");
  post = (data) => self.onmessage({ data });
});
afterEach(() => vi.unstubAllGlobals());
function start(session = 1) {
  post({
    type: "start",
    transport: "webtransport",
    url: "wss://example.test/live",
    protocols: ["sldp.softvelum.com"],
    session,
  });
}
it("selects WT and routes status/binary through the existing protocol agent", async () => {
  start();
  await tick();
  expect(transports).toHaveLength(1);
  transports[0].resolve();
  await tick();
  transports[0].controller.enqueue(Uint8Array.of(129, 2, 111, 107, 130, 1, 7));
  await tick();
  expect(agents[0].processStatus).toHaveBeenCalledWith("ok");
  expect(agents[0].processFrame).toHaveBeenCalledWith(Uint8Array.of(7).buffer);
  post({ type: "play", streams: [{ sn: 1 }] });
  post({ type: "stop", sns: [1] });
  await tick();
  const commands = transports[0].writes.map((bytes) => {
    const n = bytes[1] & 127;
    return new TextDecoder().decode(
      Uint8Array.from(
        bytes.subarray(6, 6 + n),
        (b, i) => b ^ bytes[2 + (i % 4)],
      ),
    );
  });
  expect(commands.map(JSON.parse)).toEqual([
    { command: "Play", streams: [{ sn: 1 }] },
    { command: "Cancel", streams: [1] },
  ]);
  expect(websockets).toHaveLength(0);
  post({ type: "stop", sns: [], close: true });
});
it.each(["stop", "terminate", "start"])(
  "invalidates pending WT on %s and suppresses stale events",
  async (action) => {
    start();
    await tick();
    expect(transports).toHaveLength(1);
    const old = transports[0];
    if (action === "start") start(2);
    else
      post(
        action === "stop"
          ? { type: "stop", sns: [1], close: true }
          : { type: "terminate" },
      );
    old.resolve();
    await tick();
    expect(old.close).toHaveBeenCalled();
    expect(old.createBidirectionalStream).not.toHaveBeenCalled();
    expect(self.postMessage).not.toHaveBeenCalledWith({
      aux: true,
      connected: true,
      session: 1,
    });
    if (action === "start") {
      transports[1].resolve();
      await tick();
      expect(self.postMessage).toHaveBeenCalledWith({
        aux: true,
        connected: true,
        session: 2,
      });
      post({ type: "terminate" });
    }
  },
);
it("closing stop remains safe when oversized Cancel disconnects synchronously", async () => {
  start();
  await tick();
  transports[0].resolve();
  await tick();
  expect(() =>
    post({ type: "stop", sns: ["x".repeat(5000)], close: true }),
  ).not.toThrow();
  expect(self.postMessage).toHaveBeenCalledWith({
    aux: true,
    connected: false,
    session: 1,
  });
});
