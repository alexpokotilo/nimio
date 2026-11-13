import {
  FrameDecoder,
  encodeCommand,
  encodeMasked,
} from "./webtransport-codec.js";

// A WebSocket-shaped endpoint, so the existing worker owns protocol/session state.
export class WebTransportSocket {
  constructor(url, protocols, config = {}) {
    this.readyState = 0;
    this.url = url;
    this.protocols = protocols;
    this.config = config;
    this.pendingBytes = 0;
    // Defer even synchronous constructor errors until worker handlers are installed.
    queueMicrotask(() => this.connect());
  }

  async connect() {
    if (this.readyState !== 0) return;
    const options = this.config.webTransportOptions || {};
    const requested = options.connectionTimeout;
    const timeout = Number.isFinite(requested)
      ? Math.min(30000, Math.max(1000, requested))
      : 10000;
    this.timer = setTimeout(
      () => this.fail(new Error("WebTransport connection timed out")),
      timeout,
    );
    try {
      const url = new URL(this.url);
      if (url.protocol !== "wss:")
        throw new Error("WebTransport requires a wss streamUrl");
      url.protocol = "https:";
      const wt = (this.transport = new WebTransport(
        url.href,
        options.serverCertificateHashes === undefined
          ? {}
          : { serverCertificateHashes: options.serverCertificateHashes },
      ));
      wt.closed
        .then(
          () =>
            this.transport === wt &&
            this.fail(new Error("WebTransport closed")),
          (error) => this.transport === wt && this.fail(error),
        )
        .catch(() => {});
      await wt.ready;
      if (this.transport !== wt || this.readyState !== 0) {
        wt.close();
        return;
      }
      const stream = await wt.createBidirectionalStream();
      if (this.transport !== wt || this.readyState !== 0) {
        this.discardStream(stream);
        wt.close();
        return;
      }
      this.reader = stream.readable.getReader();
      this.writer = stream.writable.getWriter();
      this.writer.closed.catch((error) => {
        if (this.transport === wt) this.fail(error);
      });
      this.reader.closed.catch(() => {});
      clearTimeout(this.timer);
      this.established = true;
      this.readyState = 1;
      this.onopen?.({ target: this });
      await this.read(wt);
    } catch (error) {
      this.fail(error);
    }
  }

  async read(wt) {
    const parser = new FrameDecoder(
      (data) => {
        if (this.readyState === 1 && this.transport === wt)
          this.onmessage?.({ target: this, data });
      },
      undefined,
      (opcode, data) => {
        if (opcode === 9) this.write(encodeMasked(data, 10));
        if (opcode === 8) {
          if (data.length === 1) throw new Error("Invalid close payload");
          if (data.length >= 2) {
            const code = new DataView(data.buffer).getUint16(0);
            if (
              !(code >= 3000 && code <= 4999) &&
              ![
                1000, 1001, 1002, 1003, 1007, 1008, 1009, 1010, 1011, 1012,
                1013, 1014,
              ].includes(code)
            )
              throw new Error("Invalid close code");
            new TextDecoder("utf-8", { fatal: true }).decode(data.subarray(2));
          }
          this.close();
        }
      },
    );
    const reader = this.reader;
    while (this.readyState === 1 && this.transport === wt) {
      const { value, done } = await reader.read();
      if (this.readyState !== 1 || this.transport !== wt) return;
      if (done) {
        parser.finish();
        this.fail(new Error("WebTransport stream ended"));
        return;
      }
      parser.push(value);
    }
  }

  send(text) {
    if (this.readyState !== 1)
      throw new DOMException("Socket is not open", "InvalidStateError");
    if (this.websocket) {
      this.websocket.send(text);
      return;
    }
    try {
      this.write(encodeCommand(text));
    } catch (error) {
      this.fail(error);
    }
  }

  write(frame) {
    if (this.readyState !== 1) return;
    // Bound queued output even when the peer stops reading.
    if (this.pendingBytes + frame.length > 65536) {
      this.fail(new Error("WebTransport write queue full"));
      return;
    }
    this.pendingBytes += frame.length;
    this.writer
      .write(frame)
      .catch((error) => this.fail(error))
      .finally(() => {
        this.pendingBytes -= frame.length;
      });
  }

  discardStream(stream) {
    Promise.resolve(stream.readable.cancel()).catch(() => {});
    Promise.resolve(stream.writable.abort()).catch(() => {});
  }

  cleanup() {
    clearTimeout(this.timer);
    const wt = this.transport;
    this.transport = undefined;
    if (wt) {
      try {
        wt.close();
      } catch {}
    }
    const reader = this.reader;
    const writer = this.writer;
    this.reader = this.writer = undefined;
    // Cancellation may remain pending under backpressure. Release ownership now;
    // outstanding read/write promises already have rejection handlers.
    if (reader) {
      reader.cancel().catch(() => {});
      reader.releaseLock();
    }
    if (writer) {
      writer.abort().catch(() => {});
      writer.releaseLock();
    }
  }

  fail(error) {
    if (this.readyState >= 2 || this.websocket) return;
    this.cleanup();
    if (!this.established && this.config.webTransportFallback === true) {
      try {
        const ws = (this.websocket = new WebSocket(this.url, this.protocols));
        ws.binaryType = "arraybuffer";
        ws.onopen = () => {
          if (this.readyState !== 0) return;
          this.readyState = 1;
          this.onopen?.({ target: this });
        };
        ws.onmessage = (event) => {
          if (this.readyState === 1)
            this.onmessage?.({ target: this, data: event.data });
        };
        ws.onclose = (event) => this.finish(event.code, event.reason);
        return;
      } catch (fallbackError) {
        error = fallbackError;
      }
    }
    this.finish(1006, error?.message || "WebTransport failed");
  }

  finish(code, reason) {
    if (this.readyState === 3) return;
    this.readyState = 3;
    this.onclose?.({ target: this, code, reason });
  }

  close() {
    if (this.readyState >= 2) return;
    this.readyState = 2;
    this.cleanup();
    if (this.websocket) {
      this.websocket.onopen =
        this.websocket.onmessage =
        this.websocket.onclose =
          null;
      this.websocket.close();
    }
    this.finish(1000, "");
  }
}
