import { beforeEach, afterEach, it, expect, vi } from "vitest";
import { ReadableStream, WritableStream } from "node:stream/web";
import { WebTransportSocket } from "@/transport/webtransport-socket";
const deferred = () => {
  let resolve, reject;
  const promise = new Promise((a, b) => {
    resolve = a;
    reject = b;
  });
  return { promise, resolve, reject };
};
const tick = async () => {
  for (let i = 0; i < 20; i++) await Promise.resolve();
};
let instances, sockets;
class WT {
  constructor(url, options) {
    this.url = url;
    this.options = options;
    this.readyGate = deferred();
    this.ready = this.readyGate.promise;
    this.closedGate = deferred();
    this.closed = this.closedGate.promise;
    this.streamGate = deferred();
    this.createBidirectionalStream = vi.fn(() => this.streamGate.promise);
    this.close = vi.fn();
    instances.push(this);
  }
}
class WS {
  constructor(url, protocols) {
    this.url = url;
    this.protocols = protocols;
    this.close = vi.fn();
    this.send = vi.fn();
    sockets.push(this);
  }
}
function stream() {
  let controller;
  const writes = [];
  const readable = new ReadableStream({
    start(c) {
      controller = c;
    },
  });
  const writable = new WritableStream({
    write(data) {
      writes.push(data);
    },
  });
  return { readable, writable, controller, writes };
}
function socket(options = {}) {
  return new WebTransportSocket(
    "wss://example.test/live?q=1",
    ["sldp.softvelum.com"],
    options,
  );
}
async function open(s) {
  await tick();
  const wt = instances.at(-1);
  const bidi = stream();
  wt.readyGate.resolve();
  wt.streamGate.resolve(bidi);
  await tick();
  return { wt, bidi };
}
beforeEach(() => {
  instances = [];
  sockets = [];
  vi.stubGlobal("WebTransport", WT);
  vi.stubGlobal("WebSocket", WS);
});
afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
});
it("maps URL, forwards certificates, opens exactly one bidi, sends framed commands and receives messages", async () => {
  const hashes = [{ algorithm: "sha-256", value: new Uint8Array(32) }];
  const s = socket({
    webTransportOptions: { serverCertificateHashes: hashes },
  });
  s.onopen = vi.fn();
  s.onmessage = vi.fn();
  const { wt, bidi } = await open(s);
  expect(wt.url).toBe("https://example.test/live?q=1");
  expect(wt.options).toEqual({ serverCertificateHashes: hashes });
  expect(wt.createBidirectionalStream).toHaveBeenCalledTimes(1);
  expect(s.onopen).toHaveBeenCalledTimes(1);
  s.send('{"command":"Play"}');
  await tick();
  expect(bidi.writes[0][0]).toBe(129);
  bidi.controller.enqueue(Uint8Array.of(129, 2, 111, 107, 130, 1, 7));
  await tick();
  expect(s.onmessage.mock.calls.map(([e]) => e.data)).toEqual([
    "ok",
    Uint8Array.of(7).buffer,
  ]);
  s.close();
  await tick();
  expect(bidi.readable.locked).toBe(false);
  expect(bidi.writable.locked).toBe(false);
});
it.each([false, true])(
  "retries initial failure only when fallback is %s",
  async (fallback) => {
    const s = socket({ webTransportFallback: fallback });
    s.onclose = vi.fn();
    await tick();
    instances[0].readyGate.reject(new Error("failed"));
    await tick();
    expect(instances[0].close).toHaveBeenCalled();
    expect(sockets).toHaveLength(fallback ? 1 : 0);
    if (fallback) {
      sockets[0].onclose({ code: 1006, reason: "failed" });
      expect(s.onclose).toHaveBeenCalledTimes(1);
    } else expect(s.onclose).toHaveBeenCalledTimes(1);
    s.close();
  },
);
it("never falls back after establishment or data", async () => {
  const s = socket({ webTransportFallback: true });
  s.onclose = vi.fn();
  const { wt, bidi } = await open(s);
  bidi.controller.enqueue(Uint8Array.of(129, 0));
  await tick();
  wt.closedGate.reject(new Error("lost"));
  await tick();
  expect(sockets).toHaveLength(0);
  expect(s.onclose).toHaveBeenCalledTimes(1);
});
it.each(["ready", "bidi"])(
  "closes resources resolving late after stop at %s",
  async (stage) => {
    const s = socket({ webTransportFallback: true });
    s.onopen = vi.fn();
    await tick();
    const wt = instances[0];
    if (stage === "bidi") {
      wt.readyGate.resolve();
      await tick();
    }
    s.close();
    const bidi = stream();
    wt.readyGate.resolve();
    wt.streamGate.resolve(bidi);
    await tick();
    expect(s.onopen).not.toHaveBeenCalled();
    expect(wt.close).toHaveBeenCalled();
    expect(sockets).toHaveLength(0);
    expect(bidi.readable.locked).toBe(false);
    expect(bidi.writable.locked).toBe(false);
    if (stage === "ready")
      expect(wt.createBidirectionalStream).not.toHaveBeenCalled();
  },
);
it("bounds the connection timeout and closes a late bidi", async () => {
  vi.useFakeTimers();
  const s = socket({
    webTransportFallback: true,
    webTransportOptions: { connectionTimeout: 1 },
  });
  await tick();
  const wt = instances[0];
  wt.readyGate.resolve();
  await tick();
  await vi.advanceTimersByTimeAsync(1000);
  expect(sockets).toHaveLength(1);
  const bidi = stream();
  wt.streamGate.resolve(bidi);
  await tick();
  expect(bidi.readable.locked).toBe(false);
  expect(bidi.writable.locked).toBe(false);
  s.close();
});
it.each([false, true])(
  "rejects insecure WS URL with fallback %s",
  async (fallback) => {
    const s = new WebTransportSocket("ws://example.test/live", [], {
      webTransportFallback: fallback,
    });
    s.onclose = vi.fn();
    await tick();
    expect(instances).toHaveLength(0);
    expect(sockets).toHaveLength(fallback ? 1 : 0);
    s.close();
  },
);
it("handles unavailable WebTransport through opt-in fallback", async () => {
  vi.stubGlobal("WebTransport", undefined);
  const s = socket({ webTransportFallback: true });
  await tick();
  expect(sockets).toHaveLength(1);
  s.close();
});
it("closes on malformed inbound frames without fallback", async () => {
  const s = socket({ webTransportFallback: true });
  s.onclose = vi.fn();
  const { bidi } = await open(s);
  bidi.controller.enqueue(Uint8Array.of(130, 128));
  await tick();
  expect(s.onclose).toHaveBeenCalledTimes(1);
  expect(sockets).toHaveLength(0);
  expect(bidi.readable.locked).toBe(false);
  expect(bidi.writable.locked).toBe(false);
});
it("falls back once when bidi creation fails and ignores later WT failures", async () => {
  const s = socket({ webTransportFallback: true });
  await tick();
  const wt = instances[0];
  wt.readyGate.resolve();
  wt.streamGate.reject(new Error("bidi failed"));
  await tick();
  expect(sockets).toHaveLength(1);
  wt.closedGate.reject(new Error("late close"));
  await tick();
  expect(sockets).toHaveLength(1);
  s.onopen = vi.fn();
  s.onmessage = vi.fn();
  sockets[0].onopen();
  sockets[0].onmessage({ data: "status" });
  s.send("Play");
  expect(s.onopen).toHaveBeenCalledTimes(1);
  expect(s.onmessage).toHaveBeenCalledWith(
    expect.objectContaining({ data: "status" }),
  );
  expect(sockets[0].send).toHaveBeenCalledWith("Play");
  s.close();
  expect(sockets[0].close).toHaveBeenCalledTimes(1);
});
it("cancels immediately before construction without creating either transport", async () => {
  const s = socket({ webTransportFallback: true });
  s.close();
  await tick();
  expect(instances).toHaveLength(0);
  expect(sockets).toHaveLength(0);
});
it("does not fall back after open even before receiving data", async () => {
  const s = socket({ webTransportFallback: true });
  s.onclose = vi.fn();
  const { bidi } = await open(s);
  bidi.controller.close();
  await tick();
  expect(sockets).toHaveLength(0);
  expect(s.onclose).toHaveBeenCalledTimes(1);
});
it.each([Infinity, NaN, undefined])(
  "uses bounded default timeout for %s",
  async (connectionTimeout) => {
    vi.useFakeTimers();
    const s = socket({ webTransportOptions: { connectionTimeout } });
    s.onclose = vi.fn();
    await tick();
    await vi.advanceTimersByTimeAsync(10000);
    expect(s.onclose).toHaveBeenCalledTimes(1);
    expect(instances[0].close).toHaveBeenCalled();
  },
);
it("caps excessive timeout at 30 seconds", async () => {
  vi.useFakeTimers();
  const s = socket({ webTransportOptions: { connectionTimeout: 9999999 } });
  s.onclose = vi.fn();
  await tick();
  await vi.advanceTimersByTimeAsync(30000);
  expect(s.onclose).toHaveBeenCalledTimes(1);
});
it("releases locks on stop even if underlying cancellation never settles", async () => {
  const s = socket();
  await tick();
  const wt = instances[0];
  const readable = new ReadableStream({ cancel: () => new Promise(() => {}) });
  const writable = new WritableStream({ abort: () => new Promise(() => {}) });
  wt.readyGate.resolve();
  wt.streamGate.resolve({ readable, writable });
  await tick();
  s.close();
  await tick();
  expect(readable.locked).toBe(false);
  expect(writable.locked).toBe(false);
});
