// RFC6455 framing only: WebTransport already establishes the HTTP/3 session.
export const MAX_MESSAGE_BYTES = 16 * 1024 * 1024;
const utf8 = new TextDecoder("utf-8", { fatal: true });

export function encodeCommand(text) {
  const payload = new TextEncoder().encode(text);
  const header = payload.length < 126 ? 2 : 4;
  if (payload.length + header + 4 > 4096)
    throw new RangeError("SLDP command exceeds 4096 bytes");
  return encodeMasked(payload, 1);
}

export function encodeMasked(payload, opcode) {
  const header = payload.length < 126 ? 2 : 4;
  const frame = new Uint8Array(header + 4 + payload.length);
  frame[0] = 0x80 | opcode;
  frame[1] = 0x80 | (header === 2 ? payload.length : 126);
  if (header === 4) new DataView(frame.buffer).setUint16(2, payload.length);
  const mask = frame.subarray(header, header + 4);
  crypto.getRandomValues(mask);
  for (let i = 0; i < payload.length; i++)
    frame[header + 4 + i] = payload[i] ^ mask[i % 4];
  return frame;
}

export class FrameDecoder {
  constructor(onmessage, limit = MAX_MESSAGE_BYTES, oncontrol = () => {}) {
    this.onmessage = onmessage;
    this.oncontrol = oncontrol;
    this.limit = limit;
    this.header = new Uint8Array(10);
    this.headerSize = 0;
    this.headerNeed = 2;
    this.remaining = null;
    this.message = new Uint8Array(0);
    this.size = 0;
    this.messageOpcode = 0;
  }

  push(bytes) {
    let offset = 0;
    while (offset < bytes.length) {
      if (this.remaining === null) {
        const count = Math.min(
          this.headerNeed - this.headerSize,
          bytes.length - offset,
        );
        this.header.set(
          bytes.subarray(offset, offset + count),
          this.headerSize,
        );
        offset += count;
        this.headerSize += count;
        if (this.headerSize < this.headerNeed) continue;
        if (this.headerNeed === 2) {
          const [first, second] = this.header;
          this.opcode = first & 15;
          this.fin = !!(first & 128);
          if (
            first & 112 ||
            second & 128 ||
            ![0, 1, 2, 8, 9, 10].includes(this.opcode)
          )
            throw new Error("Invalid RFC6455 header");
          if (this.opcode >= 8 && (!this.fin || (second & 127) > 125))
            throw new Error("Invalid control frame");
          this.headerNeed =
            (second & 127) === 126 ? 4 : (second & 127) === 127 ? 10 : 2;
          if (this.headerSize < this.headerNeed) continue;
        }
        const view = new DataView(this.header.buffer);
        let length = this.header[1] & 127;
        if (length === 126) {
          length = view.getUint16(2);
          if (length < 126) throw new Error("Noncanonical length");
        } else if (length === 127) {
          const big = view.getBigUint64(2);
          if (big < 65536n || big > BigInt(this.limit))
            throw new Error("Invalid or excessive length");
          length = Number(big);
        }
        if (this.opcode < 8) {
          if (this.opcode === 0 ? !this.messageOpcode : !!this.messageOpcode)
            throw new Error("Invalid fragmentation");
          if (this.opcode) this.messageOpcode = this.opcode;
          const needed = this.size + length;
          if (needed > this.limit) throw new Error("Message too large");
          if (needed > this.message.length) {
            const buffer = new Uint8Array(
              Math.min(
                this.limit,
                Math.max(needed, this.message.length * 2, 1024),
              ),
            );
            buffer.set(this.message.subarray(0, this.size));
            this.message = buffer;
          }
        } else {
          this.control = new Uint8Array(length);
          this.controlSize = 0;
        }
        this.remaining = length;
      }
      const count = Math.min(this.remaining, bytes.length - offset);
      if (this.opcode < 8) {
        this.message.set(bytes.subarray(offset, offset + count), this.size);
        this.size += count;
      } else {
        this.control.set(
          bytes.subarray(offset, offset + count),
          this.controlSize,
        );
        this.controlSize += count;
      }
      offset += count;
      this.remaining -= count;
      if (this.remaining === 0) {
        this.remaining = null;
        this.headerSize = 0;
        this.headerNeed = 2;
        if (this.opcode >= 8) this.oncontrol(this.opcode, this.control);
        else if (this.fin) {
          const data = this.message.slice(0, this.size);
          const opcode = this.messageOpcode;
          this.messageOpcode = 0;
          this.size = 0;
          this.message = new Uint8Array(0);
          this.onmessage(opcode === 1 ? utf8.decode(data) : data.buffer);
        }
      }
    }
  }

  finish() {
    if (this.headerSize || this.remaining !== null || this.messageOpcode)
      throw new Error("Truncated RFC6455 message");
  }
}
