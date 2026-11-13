import { describe, it, expect } from "vitest";
import { FrameDecoder, encodeCommand } from "@/transport/webtransport-codec";

function decode(chunks, limit) {
  const messages = [];
  const parser = new FrameDecoder((data) => messages.push(data), limit);
  for (const chunk of chunks) parser.push(Uint8Array.from(chunk));
  return messages;
}
describe("WebTransport RFC6455 framing", () => {
  it("masks FIN text and enforces the 4096 byte total command limit", () => {
    for (const size of [0, 125, 126, 4088]) {
      const frame = encodeCommand("x".repeat(size));
      expect(frame[0]).toBe(0x81);
      expect(frame[1] & 128).toBe(128);
      const offset = size < 126 ? 2 : 4;
      expect(frame.length).toBe(size + offset + 4);
      for (let i = 0; i < size; i++)
        expect(frame[offset + 4 + i] ^ frame[offset + (i % 4)]).toBe(120);
    }
    expect(() => encodeCommand("x".repeat(4089))).toThrow();
    expect(() => encodeCommand("é".repeat(2045))).toThrow();
  });
  it("handles split headers, coalescing, and fragmented UTF8 messages", () => {
    expect(decode([[1], [1, 0xc3, 128, 1], [0xa9, 130, 2, 7, 8]])).toEqual([
      "é",
      Uint8Array.of(7, 8).buffer,
    ]);
  });
  it("accepts large binary frames with incremental 64 bit headers", () => {
    const payload = new Uint8Array(65536).fill(42);
    const result = decode([
      [130, 127, 0],
      [0, 0, 0, 0, 1, 0, 0],
      payload.subarray(0, 40000),
      payload.subarray(40000),
    ]);
    expect(new Uint8Array(result[0])).toEqual(payload);
  });
  it.each([
    [130, 128],
    [194, 0],
    [131, 0],
    [128, 0],
    [1, 0, 129, 0],
    [130, 126, 0, 125],
    [130, 127, 128, 0, 0, 0, 0, 0, 0, 0],
    [130, 127, 0, 0, 0, 1, 0, 0, 0, 0],
    [129, 1, 255],
    [9, 0],
  ])("rejects malformed or excessive frames %j", (...bytes) => {
    expect(() => decode([bytes])).toThrow();
  });
  it("bounds aggregate fragmented message size", () => {
    expect(() => decode([[2, 3, 1, 2, 3, 128, 2, 4, 5]], 4)).toThrow();
  });
  it("rejects truncated EOF", () => {
    const parser = new FrameDecoder(() => {});
    parser.push(Uint8Array.of(130, 2, 1));
    expect(() => parser.finish()).toThrow();
  });
});
it("decodes 16 bit lengths and every byte boundary without losing coalesced frames", () => {
  const bytes = Uint8Array.from([
    130,
    126,
    0,
    126,
    ...new Uint8Array(126).fill(23),
    129,
    0,
  ]);
  const messages = decode(Array.from(bytes, (b) => [b]));
  expect(new Uint8Array(messages[0])).toEqual(new Uint8Array(126).fill(23));
  expect(messages[1]).toBe("");
});
it("handles control frames interleaved with fragmented binary data", () => {
  const messages = [],
    controls = [];
  const parser = new FrameDecoder(
    (d) => messages.push(d),
    undefined,
    (op, data) => controls.push([op, ...data]),
  );
  parser.push(Uint8Array.of(2, 1, 4, 137, 1, 9, 128, 1, 5));
  parser.finish();
  expect(messages).toEqual([Uint8Array.of(4, 5).buffer]);
  expect(controls).toEqual([[9, 9]]);
});
