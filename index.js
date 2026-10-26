const PEER_ID = /^[a-f0-9]{32}$/i;
const TOKEN = /^[A-Za-z0-9_-]{8,96}$/;
const PROFILE_ID = /^\d{12}$/;
const SECRET = /^[a-f0-9]{64}$/i;
const GLOBAL_KINDS = new Set(["friend_request", "friend_response", "dm", "dm_edit", "dm_delete", "unfriend"]);

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" }
  });
}
function isWebSocket(request) {
  return (request.headers.get("Upgrade") || "").toLowerCase() === "websocket";
}
function cleanText(value, max) {
  return String(value == null ? "" : value).replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g, "").slice(0, max);
}
function validProfile(value) { return PROFILE_ID.test(String(value || "")); }

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/health") {
      return json({ service: "ChatLite Internet Relay", status: "online", protocol: 2, features: ["global_friends", "private_messages", "offline_mailbox", "hibernation"] });
    }
    if (url.pathname === "/global") {
      if (!isWebSocket(request)) return json({ error: "websocket_required" }, 426);
      const profile = (url.searchParams.get("profile") || "").trim();
      if (!validProfile(profile)) return json({ error: "invalid_profile_id" }, 400);
      return env.CHATLITE_PROFILES.get(env.CHATLITE_PROFILES.idFromName(profile)).fetch(request);
    }
    if (url.pathname !== "/ws" && url.pathname !== "/stream") {
      return json({ error: "not_found" }, 404);
    }
    if (!isWebSocket(request)) return json({ error: "websocket_required" }, 426);
    const room = (url.searchParams.get("room") || "").trim();
    if (!TOKEN.test(room)) return json({ error: "invalid_room_token" }, 400);
    const id = env.CHATLITE_ROOMS.idFromName(room);
    return env.CHATLITE_ROOMS.get(id).fetch(request);
  }
};

// Private mailbox/presence endpoint. Each profile has its own Durable Object,
// so friend requests and DMs never get broadcast to an entire room.
export class ChatLiteProfile {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.socket = null;
    this.profileId = "";
    this.authenticated = false;
    this.profile = null;
    this.sentAt = [];

    // Restore the active socket after the Durable Object hibernates.
    // The socket remains connected while this JS object can leave memory.
    const sockets = this.state.getWebSockets();
    if (sockets.length) {
      const ws = sockets[0];
      const attachment = ws.deserializeAttachment() || {};
      this.socket = ws;
      this.profileId = String(attachment.profileId || "");
      this.authenticated = !!attachment.authenticated;
    }
  }

  async webSocketMessage(ws, message) {
    const attachment = ws.deserializeAttachment() || {};
    if (this.socket !== ws) {
      this.socket = ws;
      this.profileId = String(attachment.profileId || "");
      this.authenticated = !!attachment.authenticated;
      this.profile = null;
    }
    // Metadata is persisted because the in-memory object is recreated after hibernation.
    if (this.authenticated && !this.profile) {
      const saved = await this.state.storage.get("profile");
      if (saved && saved.id === this.profileId) this.profile = saved;
      else this.authenticated = false;
    }
    try { await this.onMessage(ws, message); }
    catch (_) { /* A malformed client message must not crash the profile mailbox. */ }
  }

  webSocketClose(ws, code, reason, wasClean) {
    this.drop(ws);
  }

  webSocketError(ws, error) {
    this.drop(ws);
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/_internal/status" && request.method === "GET") {
      const info = await this.state.storage.get("profile") || {};
      return json({ id: info.id || "", name: info.name || "Usuario", photo: info.photo || "", port: info.port || 39852,
        activity: info.activity || "", online: !!(this.authenticated && this.socket && this.socket.readyState === 1) });
    }
    if (url.pathname === "/_internal/deliver" && request.method === "POST") {
      let event;
      try { event = await request.json(); } catch (_) { return json({ error: "invalid_json" }, 400); }
      const targetId = String(event.to || "");
      const knownTargetId = (await this.state.storage.get("profile_id")) || "";
      if (!validProfile(targetId) || (knownTargetId && knownTargetId !== targetId) || !validProfile(event.from) || (!GLOBAL_KINDS.has(event.kind) && event.kind !== "dm_ack")) {
        return json({ error: "invalid_event" }, 400);
      }
      if (!knownTargetId) await this.state.storage.put("profile_id", targetId);
      const eid = cleanText(event.eid, 90);
      if (!eid) return json({ error: "missing_event_id" }, 400);
      const entry = {
        t: "event", eid, kind: event.kind, from: event.from, to: targetId,
        name: cleanText(event.name || "Usuario", 10), photo: cleanText(event.photo || "", 180000),
        port: Number.isInteger(event.port) && event.port > 0 && event.port <= 65535 ? event.port : 39852,
        messageId: cleanText(event.messageId || "", 64), body: cleanText(event.body || "", 1500),
        accepted: !!event.accepted, created: Date.now()
      };
      if (entry.kind === "dm" && (!/^[a-f0-9]{32}$/i.test(entry.messageId) || !entry.body)) return json({ error: "invalid_dm" }, 400);
      const queue = await this.state.storage.list({ prefix: "q:" });
      if (queue.size >= 200) return json({ error: "mailbox_full" }, 429);
      await this.state.storage.put("q:" + eid, entry);
      this.safeText(this.socket, JSON.stringify(entry));
      return json({ queued: true, online: !!(this.authenticated && this.socket && this.socket.readyState === 1) }, 202);
    }
    if (url.pathname !== "/global" || !isWebSocket(request)) return json({ error: "not_found" }, 404);

    const requestedProfile = (url.searchParams.get("profile") || "").trim();
    if (!validProfile(requestedProfile)) return json({ error: "invalid_profile_id" }, 400);
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    try { server.binaryType = "arraybuffer"; } catch (_) {}
    if (this.socket && this.socket !== server) {
      try { this.socket.close(4001, "new connection"); } catch (_) {}
    }
    await this.state.storage.put("profile_id", requestedProfile);
    server.serializeAttachment({ profileId: requestedProfile, authenticated: false });
    // Hibernation API keeps this connection open without pinning the DO in memory.
    this.state.acceptWebSocket(server);
    this.socket = server;
    this.profileId = requestedProfile;
    this.authenticated = false;
    this.profile = null;
    this.sentAt = [];
    return new Response(null, { status: 101, webSocket: client });
  }

  isOpen(ws) { return !!ws && ws.readyState === 1; }
  safeText(ws, value) { if (!this.isOpen(ws)) return false; try { ws.send(value); return true; } catch (_) { return false; } }
  async hashSecret(value) {
    const bytes = new TextEncoder().encode(value);
    const digest = await crypto.subtle.digest("SHA-256", bytes);
    return Array.from(new Uint8Array(digest)).map(b => b.toString(16).padStart(2, "0")).join("");
  }

  async onMessage(ws, data) {
    if (ws !== this.socket || typeof data !== "string") return;
    let message;
    try { message = JSON.parse(data); } catch (_) { return; }
    if (!this.authenticated) {
      if (message.t !== "auth" || !validProfile(message.profile) || message.profile !== await this.state.storage.get("profile_id") || !SECRET.test(String(message.secret || ""))) {
        this.safeText(ws, JSON.stringify({ t: "error", error: "auth_required" }));
        try { ws.close(1008, "authentication required"); } catch (_) {}
        return;
      }
      const secretHash = await this.hashSecret(message.secret.toLowerCase());
      const storedHash = await this.state.storage.get("secret_hash");
      if (storedHash && storedHash !== secretHash) {
        this.safeText(ws, JSON.stringify({ t: "error", error: "identity_mismatch" }));
        try { ws.close(1008, "profile identity mismatch"); } catch (_) {}
        return;
      }
      if (!storedHash) await this.state.storage.put("secret_hash", secretHash);
      this.profile = {
        id: message.profile, name: cleanText(message.name || "Usuario", 10),
        photo: cleanText(message.photo || "", 180000),
        port: Number.isInteger(message.port) && message.port > 0 && message.port <= 65535 ? message.port : 39852,
        activity: cleanText(message.activity || "", 80)
      };
      this.authenticated = true;
      this.profileId = this.profile.id;
      ws.serializeAttachment({ profileId: this.profile.id, authenticated: true });
      await this.state.storage.put("profile", this.profile);
      this.safeText(ws, JSON.stringify({ t: "auth_ok", protocol: 2 }));
      await this.deliverQueued(ws);
      return;
    }

    if (message.t === "profile_update") {
      this.profile.name = cleanText(message.name || this.profile.name, 10) || "Usuario";
      this.profile.photo = cleanText(message.photo || this.profile.photo, 180000);
      this.profile.port = Number.isInteger(message.port) && message.port > 0 && message.port <= 65535 ? message.port : 39852;
      this.profile.activity = cleanText(message.activity || "", 80);
      await this.state.storage.put("profile", this.profile);
      this.safeText(ws, JSON.stringify({ t: "profile_updated" }));
      return;
    }
    if (message.t === "presence_query") {
      const ids = Array.isArray(message.friends) ? message.friends.slice(0, 32) : [];
      for (const rawId of ids) {
        const friendId = String(rawId || "");
        if (!validProfile(friendId) || friendId === this.profile.id) continue;
        try {
          const stub = this.env.CHATLITE_PROFILES.get(this.env.CHATLITE_PROFILES.idFromName(friendId));
          const response = await stub.fetch("https://internal/_internal/status");
          const status = await response.json();
          this.safeText(ws, JSON.stringify({ t: "presence", id: friendId, online: !!status.online,
            name: cleanText(status.name || "Usuario", 10), photo: cleanText(status.photo || "", 180000),
            port: status.port || 39852, activity: cleanText(status.activity || "", 80) }));
        } catch (_) {
          this.safeText(ws, JSON.stringify({ t: "presence", id: friendId, online: false, name: "Usuario", photo: "", port: 39852, activity: "" }));
        }
      }
      return;
    }
    if (message.t === "ack") {
      const eid = cleanText(message.eid || "", 90);
      if (!eid) return;
      const queued = await this.state.storage.get("q:" + eid);
      if (queued) {
        await this.state.storage.delete("q:" + eid);
        if (queued.kind === "dm" && message.delivered) {
          await this.routeEvent({ kind: "dm_ack", from: this.profile.id, to: queued.from,
            name: this.profile.name, photo: this.profile.photo, port: this.profile.port,
            messageId: queued.messageId, body: "", accepted: true });
        }
      }
      return;
    }
    if (message.t !== "send") return;
    const kind = String(message.kind || "");
    const to = String(message.to || "");
    if (!GLOBAL_KINDS.has(kind) || !validProfile(to) || to === this.profile.id) {
      this.safeText(ws, JSON.stringify({ t: "send_result", kind, id: message.messageId || "", queued: false, error: "invalid_target" }));
      return;
    }
    const now = Date.now();
    this.sentAt = this.sentAt.filter(stamp => now - stamp < 60000);
    if (this.sentAt.length >= 90) {
      this.safeText(ws, JSON.stringify({ t: "send_result", kind, id: message.messageId || "", queued: false, error: "rate_limited" }));
      return;
    }
    this.sentAt.push(now);
    const body = cleanText(message.body || "", 1500);
    const messageId = cleanText(message.messageId || "", 64);
    if ((kind === "dm" || kind === "dm_edit") && !body || (kind === "dm" && !/^[a-f0-9]{32}$/i.test(messageId))) {
      this.safeText(ws, JSON.stringify({ t: "send_result", kind, id: messageId, queued: false, error: "invalid_payload" }));
      return;
    }
    if ((kind === "dm_edit" || kind === "dm_delete") && !/^[a-f0-9]{32}$/i.test(messageId)) {
      this.safeText(ws, JSON.stringify({ t: "send_result", kind, id: messageId, queued: false, error: "invalid_message_id" }));
      return;
    }
    const event = {
      kind, from: this.profile.id, to,
      name: this.profile.name,
      photo: kind === "friend_request" || kind === "friend_response" ? this.profile.photo : "",
      port: this.profile.port,
      messageId,
      body: (kind === "dm" || kind === "dm_edit") ? body : "",
      accepted: !!message.accepted
    };
    const result = await this.routeEvent(event);
    this.safeText(ws, JSON.stringify({ t: "send_result", kind, id: messageId, queued: !!result.queued, error: result.error || "" }));
  }

  async routeEvent(event) {
    const eid = Date.now().toString(36) + "-" + crypto.randomUUID();
    const target = this.env.CHATLITE_PROFILES.get(this.env.CHATLITE_PROFILES.idFromName(event.to));
    const requestBody = Object.assign({ eid }, event);
    try {
      const response = await target.fetch("https://internal/_internal/deliver", {
        method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(requestBody)
      });
      if (!response.ok) return { queued: false, error: "target_unavailable" };
      const result = await response.json();
      return { queued: !!result.queued };
    } catch (_) { return { queued: false, error: "target_unavailable" }; }
  }

  async deliverQueued(ws) {
    const queue = await this.state.storage.list({ prefix: "q:" });
    const ordered = Array.from(queue.values()).sort((a, b) => (a.created || 0) - (b.created || 0));
    for (const event of ordered) {
      if (!this.isOpen(ws) || ws !== this.socket || !this.authenticated) break;
      this.safeText(ws, JSON.stringify(event));
    }
  }

  drop(ws) {
    if (this.socket === ws) {
      this.socket = null;
      this.authenticated = false;
      this.profile = null;
    }
  }
}

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

    if (!isWebSocket(request)) return json({ error: "websocket_required" }, 426);
    if (path === "/ws") {
      if (role !== "host" && role !== "guest") return json({ error: "invalid_role" }, 400);
      if (role === "host" && this.isOpen(this.host)) return json({ error: "host_already_connected" }, 409);
      if (role === "guest") {
        if (!this.isOpen(this.host)) return json({ error: "room_not_active" }, 409);
        if (!PEER_ID.test(peer)) return json({ error: "invalid_peer" }, 400);
        if (this.guests.size >= 6) return json({ error: "room_full" }, 409);
        if (this.isOpen(this.guests.get(peer))) return json({ error: "peer_already_connected" }, 409);
      }
    } else if (path === "/stream") {
      if (role !== "hoststream" && role !== "gueststream") return json({ error: "invalid_role" }, 400);
      if (!PEER_ID.test(peer) || !PEER_ID.test(streamId)) return json({ error: "invalid_stream" }, 400);
      if (role === "gueststream" && !this.isOpen(this.guests.get(peer))) return json({ error: "guest_control_missing" }, 409);
      if (role === "hoststream" && !this.isOpen(this.host)) return json({ error: "host_control_missing" }, 409);
    } else return json({ error: "not_found" }, 404);

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

  isOpen(ws) { return !!ws && ws.readyState === 1; }
  safeText(ws, value) { if (!this.isOpen(ws)) return false; try { ws.send(value); return true; } catch (_) { return false; } }
  safeData(ws, value) { if (!this.isOpen(ws)) return false; try { ws.send(value); return true; } catch (_) { return false; } }
  pairStream(streamId, entry) {
    if (!entry || entry.ready || !this.isOpen(entry.host) || !this.isOpen(entry.guest)) return;
    entry.ready = true;
    const h = this.clients.get(entry.host), g = this.clients.get(entry.guest);
    if (h) h.ready = true; if (g) g.ready = true;
    this.safeText(entry.host, "READY"); this.safeText(entry.guest, "READY");
  }
  onMessage(ws, data) {
    const record = this.clients.get(ws); if (!record) return;
    if (record.role === "hoststream" || record.role === "gueststream") {
      const entry = this.streams.get(record.streamId); if (!entry || !entry.ready) return;
      const target = record.role === "hoststream" ? entry.guest : entry.host;
      if (this.isOpen(target)) this.safeData(target, data); return;
    }
    if (typeof data === "string") {
      let message; try { message = JSON.parse(data); } catch (_) { return; }
      const type = message && message.t;
      if (record.role === "guest") {
        if (type === "open" && PEER_ID.test((message.peer || "").toLowerCase()) && PEER_ID.test((message.stream || "").toLowerCase()))
          this.safeText(this.host, JSON.stringify({ t: "open", peer: message.peer.toLowerCase(), stream: message.stream.toLowerCase() }));
        else if (type === "leave") { this.safeText(this.host, JSON.stringify({ t: "leave", peer: record.peer })); this.closePeerStreams(record.peer, "guest left"); }
      } else if (record.role === "host") {
        if (type === "tcp_error" && PEER_ID.test((message.peer || "").toLowerCase()))
          this.safeText(this.guests.get(message.peer.toLowerCase()), JSON.stringify({ t: "tcp_error", peer: message.peer, stream: message.stream || "" }));
        else if (type === "host_close") {
          for (const [id, guest] of this.guests) { this.safeText(guest, JSON.stringify({ t: "host_close" })); try { guest.close(1001, "host closed room"); } catch (_) {} this.closePeerStreams(id, "host closed room"); }
          this.guests.clear();
        }
      }
      return;
    }
    if (!(data instanceof ArrayBuffer)) return;
    const bytes = new Uint8Array(data); if (bytes.length < 33) return;
    const kind = bytes[0]; let peer = "";
    for (let i = 1; i < 33; i++) peer += String.fromCharCode(bytes[i]);
    peer = peer.toLowerCase(); if (!PEER_ID.test(peer)) return;
    if (record.role === "guest" && kind === 2) this.safeData(this.host, data);
    else if (record.role === "host" && kind === 3) this.safeData(this.guests.get(peer), data);
  }
  closePeerStreams(peer, reason) {
    const toClose = []; for (const [id, entry] of this.streams) if (entry.peer === peer) toClose.push(id);
    for (const id of toClose) { const entry = this.streams.get(id); if (entry) { try { if (entry.host) entry.host.close(1001, reason || "closed"); } catch (_) {} try { if (entry.guest) entry.guest.close(1001, reason || "closed"); } catch (_) {} } this.streams.delete(id); }
  }
  drop(ws) {
    const record = this.clients.get(ws); if (!record) return; this.clients.delete(ws);
    if (record.role === "host") {
      if (this.host === ws) this.host = null;
      for (const [peer, guest] of this.guests) { this.safeText(guest, JSON.stringify({ t: "host_close" })); try { guest.close(1001, "host disconnected"); } catch (_) {} this.closePeerStreams(peer, "host disconnected"); }
      this.guests.clear();
      for (const entry of this.streams.values()) { try { if (entry.host) entry.host.close(1001, "host disconnected"); } catch (_) {} try { if (entry.guest) entry.guest.close(1001, "host disconnected"); } catch (_) {} }
      this.streams.clear(); return;
    }
    if (record.role === "guest") { if (this.guests.get(record.peer) === ws) this.guests.delete(record.peer); this.safeText(this.host, JSON.stringify({ t: "leave", peer: record.peer })); this.closePeerStreams(record.peer, "guest disconnected"); return; }
    const entry = this.streams.get(record.streamId);
    if (entry) { if (entry.host === ws) entry.host = null; if (entry.guest === ws) entry.guest = null; this.streams.delete(record.streamId); try { if (entry.host) entry.host.close(1001, "stream closed"); } catch (_) {} try { if (entry.guest) entry.guest.close(1001, "stream closed"); } catch (_) {} }
  }
}
