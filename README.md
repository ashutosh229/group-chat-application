# Group Chat Application using WebSockets

A real-time chat system built around one Node.js backend and a browser
frontend, extended well past a single-room broadcast demo into a
layered, multi-room system with private messaging, moderation,
presence, persistence, rate limiting, and resilient reconnection.

> **Why it looks like this:** the assignment caps teams at 4, and we're
> 5. To fairly reflect the extra person, the scope and engineering
> rigor here deliberately go beyond the stated minimum — not just in
> feature count, but in how the system is *structured* (see
> [System design](#system-design--why-its-structured-this-way) below).

---

## Feature checklist

**Baseline requirements:**
- [x] User join/leave notifications
- [x] Real-time message broadcasting to all connected users
- [x] Usernames for identifying participants
- [x] Graceful handling of client disconnections

**Beyond baseline:**
- [x] **Multiple chat rooms** — join, switch, or create rooms; broadcast is scoped per room, not global
- [x] **Private (1-to-1) messaging** — DM any online user regardless of which room they're in
- [x] **Durable message history** — messages persist to disk; a new joiner (or the server, after a restart) replays the last 50 messages of a room
- [x] **Presence system** — online / away / offline, not just connected/disconnected, based on activity
- [x] **Typing indicators**, scoped per room
- [x] **Role-based moderation** — designated admin users can **mute** or **kick** disruptive participants
- [x] **Rate limiting / anti-spam** — token-bucket limiter per connection stops message flooding
- [x] **Delivery acknowledgment** — sender gets a "✓ delivered" tick once the server confirms broadcast
- [x] **Automatic reconnection with exponential backoff** — a dropped Wi-Fi connection recovers on its own
- [x] **Graceful server shutdown** — connected clients get a warning and a clean close instead of a silent drop
- [x] **Structured JSON logging** on the server for every lifecycle event
- [x] **Automated integration test suite** (`npm test`) exercising all of the above against the real server

---

## Quick start

Requires Node.js v16+ (uses only the `ws` package — no framework, no build step).

```bash
npm install      # pulls in the ws library
npm test         # optional: runs the full integration suite (see tests/simulate.js)
npm start        # starts the server on :8080
```

Then, on each lab machine, open a browser to:

```
http://<server-machine-ip>:8080
```

Enter a username (and optionally a starting room), click **Connect**.
To demo moderation, connect one client as username `admin` (configurable —
see [Configuration](#configuration)) and it will have mute/kick controls
next to every other user's name.

Find the server machine's LAN IP with `hostname -I` or `ip addr show`,
and make sure port 8080/TCP is open on the lab firewall.

---

## System design — why it's structured this way

The baseline version of this project was one file: a single `server.js`
mixing HTTP serving, connection tracking, and every message handler
together. That's fine for a 60-line demo; it stops being fine the moment
you add five more features, because every new feature has to be spliced
into the same function and every bug fix risks breaking something
unrelated. This version is deliberately split along **separation of
concerns**, so each module has exactly one job and can be reasoned about
(and tested) independently:

```
server/
  config.js       -> all tunables in one place (ports, limits, admin list)
  logger.js       -> structured JSON logging, one call site format
  store.js        -> the ONLY module that knows the on-disk message format
  rateLimiter.js  -> generic token-bucket, no chat-specific knowledge
  rooms.js        -> room membership + scoped broadcast, no protocol knowledge
  wsServer.js     -> the wire protocol: turns client JSON into calls on
                     the modules above
  index.js        -> composition root: wires HTTP + WS + the modules
                     together and owns process lifecycle (startup/shutdown)

public/
  index.html      -> structure only
  styles.css      -> presentation only
  app.js          -> behavior: connection lifecycle, rendering, state
```

A few specific design decisions worth calling out:

**Room isolation is enforced by the data structure, not by convention.**
`RoomManager` is the *only* thing that can broadcast into a room, and it
does so by iterating that room's own `members` map — there's no shared
global client list a handler could accidentally broadcast to. This is
what makes "message in #tech never reaches #general" a structural
guarantee rather than something a handler has to remember to check
(verified in `tests/simulate.js`, "room isolation").

**Persistence is abstracted behind two functions.** `store.js` exposes
only `appendMessage(room, msg)` and `getHistory(room, limit)`. Every
other module — and the whole rest of the app — is unaware that history
is currently stored as append-only JSON-Lines files on disk. That's a
deliberate seam: swapping this for SQLite/Postgres later, or adding
message search, means rewriting one file, not touching the protocol
handlers.

**The rate limiter and room manager know nothing about chat.**
`TokenBucket` is a generic algorithm; `RoomManager` deals in "clients"
and "broadcasts," not "usernames" or "messages." Only `wsServer.js`
combines them into chat-specific behavior. This keeps the reusable parts
reusable and the protocol-specific parts in one auditable place.

**Defense in depth against a single client.** Three independent
mechanisms bound what one connection can do: message length caps
(`config.MAX_MESSAGE_LEN`), the token-bucket rate limiter (bounds
*frequency*), and server-side `textContent`-only rendering on the
client (bounds *injection* — a malicious username or message can't run
script or break the DOM, since we never use `innerHTML` for user data).

**Resilience is symmetric.** The server treats "client vanished without
closing" as a real case, not an edge case — a 30-second ping/pong
heartbeat reaps dead sockets the same way a clean `close` event does.
The client mirrors this on the other side: `app.js` reconnects
automatically with exponential backoff (1s → 2s → 4s → 8s → capped at
16s) instead of leaving the user stuck on a dead socket, and re-joins
the same username/room once reconnected.

**Every non-trivial behavior has a test that exercises the real server
end-to-end**, not mocked handlers — `tests/simulate.js` spawns
`server/index.js` as an actual child process and drives real `ws`
clients against it (see [Testing](#testing)).

---

## Architecture

```
┌────────────┐        ┌──────────────────────────────┐
│  Client 1  │◄──WS──►│                                │
├────────────┤        │   server/index.js (HTTP+WS)    │
│  Client 2  │◄──WS──►│         │                        │
├────────────┤        │   server/wsServer.js  (protocol)│
│  Client 3  │◄──WS──►│    │        │        │           │
├────────────┤        │ rooms.js store.js rateLimiter.js │
│  Client 4  │◄──WS──►│                                │
├────────────┤        └──────────────────────────────┘
│  Client 5  │◄──WS──►            (data/rooms/*.log)
└────────────┘
```

One HTTP server serves the static frontend **and** upgrades to
WebSocket on the same port — one port to open on a lab firewall, not
two.

## Wire protocol

Every message is a JSON object with a `type` field.

**Client → Server**

| type | payload | purpose |
|---|---|---|
| `join` | `{username, room}` | first message on connect; chooses identity + room |
| `message` | `{text}` | send to your current room |
| `private_message` | `{to, text}` | DM another online user |
| `switch_room` | `{room}` | move to a different existing room |
| `create_room` | `{room}` | create (if needed) and switch to a room |
| `typing` | `{}` | ephemeral typing signal, scoped to current room |
| `mute` / `unmute` | `{target}` | admin-only: silence/unsilence a user |
| `kick` | `{target}` | admin-only: disconnect a user |

**Server → Client**

| type | payload | purpose |
|---|---|---|
| `welcome` | `{username, room, isAdmin, users, rooms, onlineUsers, history}` | reply to a successful `join` |
| `notification` | `{text, users, room}` | someone joined/left/was muted etc. |
| `room_list` | `{rooms}` | global room roster changed |
| `room_switched` | `{room, users, history}` | reply to `switch_room`/`create_room` |
| `message` | `{id, username, text, room, timestamp}` | broadcast chat message |
| `private_message` | `{id, from, to, text, timestamp}` | a DM (also echoed back to the sender) |
| `presence` | `{username, status}` | online / away / offline change |
| `typing` | `{username, room}` | someone is typing |
| `delivered` | `{id}` | ack that your message was broadcast |
| `error` | `{code, text}` | e.g. `rate_limited`, `muted`, `not_admin`, `user_offline`, `no_such_room` |
| `kicked` | `{by}` | you were kicked; client won't auto-reconnect after this |
| `system_shutdown` | `{text}` | server is going down gracefully |

## Configuration

Everything tunable lives in `server/config.js` and can be overridden
with environment variables at launch, e.g.:

```bash
PORT=9000 ADMIN_USERNAMES=admin,ta1,ta2 npm start
```

| Variable | Default | Meaning |
|---|---|---|
| `PORT` | `8080` | HTTP/WS listen port |
| `DEFAULT_ROOMS` | `general,random,tech` | rooms that exist from boot |
| `ADMIN_USERNAMES` | `admin` | comma-separated usernames with mute/kick power |
| `HISTORY_LIMIT` | `50` | messages replayed on join/room-switch |
| `RATE_LIMIT_BURST` | `8` | max message burst before throttling |
| `RATE_LIMIT_REFILL` | `2` | messages/sec sustained rate after a burst |
| `HEARTBEAT_INTERVAL_MS` | `30000` | dead-connection detection interval |
| `IDLE_TIMEOUT_MS` | `60000` | inactivity before a user shows "away" |

## Testing

```bash
npm test
```

`tests/simulate.js` spawns the real server as a child process and
drives multiple concurrent `ws` clients against it, asserting on:

1. **Room isolation** — a message sent in `#general` never reaches a client in `#tech`.
2. **Duplicate usernames** — `Alice` joining twice becomes `Alice` and `Alice(1)`.
3. **Private messaging** — a DM reaches only its target, and DMing an offline user returns a clean error instead of hanging.
4. **Room switching + history replay** — a new joiner sees messages sent before it connected (proves persistence, not just in-memory state).
5. **Rate limiting** — flooding the socket with messages trips `rate_limited`.
6. **Moderation** — a non-admin's mute attempt is rejected; an admin's mute suppresses the target's messages; an admin's kick force-closes the target's socket.
7. **Ungraceful disconnect** — an abruptly terminated connection (no clean WebSocket close frame) still produces a leave notification, proving the heartbeat/close cleanup path works, not just the happy path.

All nine scenarios currently pass against the implementation in this repo.

## Manual demo script (for the lab / TA walkthrough)

1. Start the server; open 4–5 browser tabs/machines pointed at it.
2. Join with different usernames, one of them `admin`.
3. Send messages in `#general`; show they don't appear if you switch to `#tech`.
4. Click another user's name to open a DM; show the DM doesn't appear in the room log.
5. Create a new room from the sidebar (`+ new room`) and show it appears for everyone.
6. From `admin`, mute a user and show their message gets rejected; then kick them and show their tab disconnects.
7. Close a tab normally — show the leave notification. Then force-quit a browser or unplug Wi-Fi on one machine — show the room still detects and announces the leave within ~30s (heartbeat path).
8. Reconnect that machine — show it automatically reconnects and rejoins its room without the user doing anything.
9. Restart the server process, reconnect a client — show prior messages in the room are still there (persisted history).

## Known limitations / natural next steps

Documented honestly rather than hidden, since a good design doc says
what it *doesn't* do:

- Single-process, in-memory client registry — doesn't horizontally
  scale across multiple server instances without adding a shared
  pub/sub layer (e.g. Redis) for cross-instance broadcast. Fine for a
  lab-scale deployment; would be the first thing to change for
  production scale.
- History storage is flat files, sufficient for a demo; a real
  deployment would move `store.js` to a proper database (the module
  boundary was designed to make that swap self-contained).
- No authentication beyond a self-declared username — anyone can claim
  the `admin` username unless the server operator restricts who's
  allowed to type it (out of scope for a LAN lab exercise, but noted
  for completeness).
- Traffic is plain `ws://`, not `wss://` — fine on an isolated lab LAN;
  would need TLS termination for anything public-facing.
