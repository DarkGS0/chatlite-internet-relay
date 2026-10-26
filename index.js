const PEER_ID = /^[a-f0-9]{32}$/i;
const TOKEN = /^[A-Za-z0-9_-]{8,96}$/;

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" }
  });
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/health") {
      return json({ service: "ChatLite Internet Relay", status: "online", protocol: 1 });
    }
    if (url.pathname !== "/ws" && url.pathname !== "/stream") {
      return json({ error: "not_found" }, 404);
    }
    if ((request.headers.get("Upgrade") || "").toLowerCase() !== "websocket") {
      return json({ error: "websocket_required" }, 426);
    }

    const room = (url.searchParams.get("room") || "").trim();
    if (!TOKEN.test(room)) return json({ error: "invalid_room_token" }, 400);

    const id = env.CHATLITE_ROOMS.idFromName(room);
    const stub = env.CHATLITE_ROOMS.get(id);
    return stub.fetch(request);
  }
};

export class ChatLiteRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.clients = new Map();
    this.guests = new Map();
    this.streams = new Map();
    this.host = null;
  }

  async fetch(request) {
    const url = new URL(request.url);
    const path = url.pathname;
    const role = url.searchParams.get("role") || "";
    const peer = (url.searchParams.get("peer") || "").toLowerCase();
    const streamId = (url.searchParams.get("stream") || "").toLowerCase();

    if (path === "/ws") {
      if (role !== "host" && role !== "guest") return json({ error: "invalid_role" }, 400);
      if (role === "host" && this.isOpen(this.host)) return json({ error: "host_already_connected" }, 409);
      if (role === "guest") {
        if (!this.isOpen(this.host)) return json({ error: "room_not_active" }, 409);
        if (!PEER_ID.test(peer)) return json({ error: "invalid_peer" }, 400);
        if (this.guests.size >= 6) return json({ error: "room_full" }, 409);
        if (this.isOpen(this.guests.get(peer))) return json({ error: "peer_already_connected" }, 409);
      }
    } else {
      if (role !== "hoststream" && role !== "gueststream") return json({ error: "invalid_role" }, 400);
      if (!PEER_ID.test(peer) || !PEER_ID.test(streamId)) return json({ error: "invalid_stream" }, 400);
      if (role === "gueststream" && !this.isOpen(this.guests.get(peer))) return json({ error: "guest_control_missing" }, 409);
      if (role === "hoststream" && !this.isOpen(this.host)) return json({ error: "host_control_missing" }, 409);
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    server.accept();
    try { server.binaryType = "arraybuffer"; } catch (_) {}

    const record = { ws: server, role, peer, streamId, ready: false };
    this.clients.set(server, record);
    server.addEventListener("message", event => this.onMessage(server, event.data));
    server.addEventListener("close", () => this.drop(server));
    server.addEventListener("error", () => this.drop(server));

    if (role === "host") {
      this.host = server;
      this.safeText(server, JSON.stringify({ t: "ready", role: "host" }));
    } else if (role === "guest") {
      this.guests.set(peer, server);
      this.safeText(server, JSON.stringify({ t: "ready", role: "guest" }));
    } else {
      let entry = this.streams.get(streamId);
      if (!entry) {
        entry = { peer, host: null, guest: null, ready: false };
        this.streams.set(streamId, entry);
      }
      if (entry.peer !== peer) {
        try { server.close(1008, "stream peer mismatch"); } catch (_) {}
        this.clients.delete(server);
        return json({ error: "stream_peer_mismatch" }, 409);
      }
      if (role === "hoststream") {
        if (entry.host && entry.host !== server) { try { entry.host.close(1012, "stream replaced"); } catch (_) {} }
        entry.host = server;
      } else {
        if (entry.guest && entry.guest !== server) { try { entry.guest.close(1012, "stream replaced"); } catch (_) {} }
        entry.guest = server;
      }
      this.pairStream(streamId, entry);
    }

    return new Response(null, { status: 101, webSocket: client });
  }

  isOpen(ws) {
    return !!ws && ws.readyState === 1;
  }

  safeText(ws, value) {
    if (!this.isOpen(ws)) return false;
    try { ws.send(value); return true; } catch (_) { return false; }
  }

  safeData(ws, value) {
    if (!this.isOpen(ws)) return false;
    try { ws.send(value); return true; } catch (_) { return false; }
  }

  pairStream(streamId, entry) {
    if (!entry || entry.ready || !this.isOpen(entry.host) || !this.isOpen(entry.guest)) return;
    entry.ready = true;
    const h = this.clients.get(entry.host);
    const g = this.clients.get(entry.guest);
    if (h) h.ready = true;
    if (g) g.ready = true;
    this.safeText(entry.host, "READY");
    this.safeText(entry.guest, "READY");
  }

  onMessage(ws, data) {
    const record = this.clients.get(ws);
    if (!record) return;

    if (record.role === "hoststream" || record.role === "gueststream") {
      const entry = this.streams.get(record.streamId);
      if (!entry || !entry.ready) return;
      const target = record.role === "hoststream" ? entry.guest : entry.host;
      if (this.isOpen(target)) this.safeData(target, data);
      return;
    }

    if (typeof data === "string") {
      let message;
      try { message = JSON.parse(data); } catch (_) { return; }
      const type = message && message.t;
      if (record.role === "guest") {
        if (type === "open" && PEER_ID.test((message.peer || "").toLowerCase()) && PEER_ID.test((message.stream || "").toLowerCase())) {
          this.safeText(this.host, JSON.stringify({ t: "open", peer: message.peer.toLowerCase(), stream: message.stream.toLowerCase() }));
        } else if (type === "leave") {
          this.safeText(this.host, JSON.stringify({ t: "leave", peer: record.peer }));
          this.closePeerStreams(record.peer, "guest left");
        }
      } else if (record.role === "host") {
        if (type === "tcp_error" && PEER_ID.test((message.peer || "").toLowerCase())) {
          this.safeText(this.guests.get(message.peer.toLowerCase()), JSON.stringify({ t: "tcp_error", peer: message.peer, stream: message.stream || "" }));
        } else if (type === "host_close") {
          for (const [id, guest] of this.guests) {
            this.safeText(guest, JSON.stringify({ t: "host_close" }));
            try { guest.close(1001, "host closed room"); } catch (_) {}
            this.closePeerStreams(id, "host closed room");
          }
          this.guests.clear();
        }
      }
      return;
    }

    if (!(data instanceof ArrayBuffer)) return;
    const bytes = new Uint8Array(data);
    if (bytes.length < 33) return;
    const kind = bytes[0];
    let peer = "";
    for (let i = 1; i < 33; i++) peer += String.fromCharCode(bytes[i]);
    peer = peer.toLowerCase();
    if (!PEER_ID.test(peer)) return;

    if (record.role === "guest" && kind === 2) {
      this.safeData(this.host, data);
    } else if (record.role === "host" && kind === 3) {
      this.safeData(this.guests.get(peer), data);
    }
  }

  closePeerStreams(peer, reason) {
    const toClose = [];
    for (const [id, entry] of this.streams) if (entry.peer === peer) toClose.push(id);
    for (const id of toClose) {
      const entry = this.streams.get(id);
      if (entry) {
        try { if (entry.host) entry.host.close(1001, reason || "closed"); } catch (_) {}
        try { if (entry.guest) entry.guest.close(1001, reason || "closed"); } catch (_) {}
      }
      this.streams.delete(id);
    }
  }

  drop(ws) {
    const record = this.clients.get(ws);
    if (!record) return;
    this.clients.delete(ws);

    if (record.role === "host") {
      if (this.host === ws) this.host = null;
      for (const [peer, guest] of this.guests) {
        this.safeText(guest, JSON.stringify({ t: "host_close" }));
        try { guest.close(1001, "host disconnected"); } catch (_) {}
        this.closePeerStreams(peer, "host disconnected");
      }
      this.guests.clear();
      for (const [id, entry] of this.streams) {
        try { if (entry.host) entry.host.close(1001, "host disconnected"); } catch (_) {}
        try { if (entry.guest) entry.guest.close(1001, "host disconnected"); } catch (_) {}
      }
      this.streams.clear();
      return;
    }

    if (record.role === "guest") {
      if (this.guests.get(record.peer) === ws) this.guests.delete(record.peer);
      this.safeText(this.host, JSON.stringify({ t: "leave", peer: record.peer }));
      this.closePeerStreams(record.peer, "guest disconnected");
      return;
    }

    const entry = this.streams.get(record.streamId);
    if (entry) {
      if (entry.host === ws) entry.host = null;
      if (entry.guest === ws) entry.guest = null;
      this.streams.delete(record.streamId);
      try { if (entry.host) entry.host.close(1001, "stream closed"); } catch (_) {}
      try { if (entry.guest) entry.guest.close(1001, "stream closed"); } catch (_) {}
    }
  }
}
