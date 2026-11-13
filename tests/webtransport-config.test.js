import { it, expect, vi } from "vitest";
import { createConfig } from "@/player-config";
import { SLDPManager } from "@/sldp/manager";
const base = { streamUrl: "wss://example.test/live", container: "player" };
it("defaults to WebSocket without fallback", () => {
  const c = createConfig(base);
  expect(c.transport).toBe("websocket");
  expect(c.webTransportFallback).toBe(false);
});
it("passes explicit transport options from config through manager start", () => {
  const c = createConfig({
    ...base,
    transport: "webtransport",
    webTransportFallback: true,
    webTransportOptions: {
      connectionTimeout: 5000,
      serverCertificateHashes: [],
    },
  });
  const transport = { send: vi.fn(), setCallback: vi.fn() };
  const m = new SLDPManager("wt-config");
  m.init(transport, c);
  m.start(c.streamUrl);
  expect(transport.send).toHaveBeenCalledWith(
    "start",
    expect.objectContaining({
      transport: "webtransport",
      webTransportFallback: true,
      webTransportOptions: {
        connectionTimeout: 5000,
        serverCertificateHashes: [],
      },
    }),
  );
});
it.each([{ transport: "quic" }, { webTransportFallback: "true" }])(
  "rejects ambiguous transport config %j",
  (extra) => {
    expect(() => createConfig({ ...base, ...extra })).toThrow();
  },
);
