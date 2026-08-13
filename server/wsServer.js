/**
 * wsServer.js
 * ------------------------------------------------------------
 * Wires together RoomManager, the rate limiter, and the durable
 * store into the actual WebSocket protocol handlers. This is the
 * only file that understands the wire format (the `type` field
 * on every JSON message) — config/store/rooms stay generic and
 * reusable.
 *
 * Wire protocol (client -> server):
 *   { type:'join',            username, room }
 *   { type:'message',         text }
 *   { type:'private_message', to, text }
 *   { type:'switch_room',     room }
 *   { type:'create_room',     room }
 *   { type:'typing' }
 *   { type:'kick',            target }      (admin only)
 *   { type:'mute'/'unmute',   target }      (admin only)
 *
 * Wire protocol (server -> client):
 *   { type:'welcome',         username, room, isAdmin, users, rooms, history }
 *   { type:'notification',    text, users, room }
 *   { type:'message',         id, username, text, room, timestamp }
 *   { type:'private_message', id, from, to, text, timestamp }
 *   { type:'room_switched',   room, users, history }
 *   { type:'room_list',       rooms }
 *   { type:'presence',        username, status }   status: online|away|offline
 *   { type:'typing',          username, room }
 *   { type:'delivered',       id }
 *   { type:'error',           text, code }
 *   { type:'kicked',          by }
 *   { type:'system_shutdown', text }
 * ------------------------------------------------------------
 */

const { randomUUID } = require('crypto');
const WebSocket = require('ws');
const config = require('./config');
const store = require('./store');
const { TokenBucket } = require('./rateLimiter');

function attachWsServer(wss, roomManager, logger) {
  /** @type {Map<string, ClientRecord>} clientId -> record */
  const clientsById = new Map();
  /** @type {Map<string, string>} lowercase username -> clientId, for O(1) DM/kick lookup */
  const usernameToId = new Map();

  for (const room of config.DEFAULT_ROOMS) roomManager.ensureRoom(room);

  function isValidRoomName(name) {
    return typeof name === 'string' && /^[a-zA-Z0-9_-]{1,24}$/.test(name);
  }

  function broadcastToAll(obj, excludeClientId = null) {
    const payload = JSON.stringify(obj);
    for (const [id, c] of clientsById) {
      if (id === excludeClientId) continue;
      if (c.ws.readyState === WebSocket.OPEN) c.ws.send(payload);
    }
  }

  function sendError(ws, code, text) {
    ws.send(JSON.stringify({ type: 'error', code, text }));
  }

  function broadcastPresence(client, status) {
    broadcastToAll({ type: 'presence', username: client.username, status });
  }

  function uniqueUsername(requested) {
    const taken = new Set(usernameToId.keys());
    let base = requested;
    let final = base;
    let n = 1;
    while (taken.has(final.toLowerCase())) final = `${base}(${n++})`;
    return final;
  }

  // ---------------------------------------------------------
  // Per-connection handlers
  // ---------------------------------------------------------
  wss.on('connection', (ws) => {
    const clientId = randomUUID();
    ws.isAlive = true;

    // Client hasn't joined (chosen a username/room) yet.
    const client = {
      id: clientId,
      ws,
      username: null,
      room: null,
      isAdmin: false,
      muted: false,
      presence: 'online',
      lastActivity: Date.now(),
      bucket: new TokenBucket(config.RATE_LIMIT.BURST, config.RATE_LIMIT.REFILL_PER_SEC),
    };
    clientsById.set(clientId, client);

    ws.on('pong', () => {
      ws.isAlive = true;
    });

    ws.on('message', (raw) => {
      let data;
      try {
        data = JSON.parse(raw);
      } catch {
        return; // ignore malformed frames
      }
      client.lastActivity = Date.now();
      if (client.presence === 'away') {
        client.presence = 'online';
        broadcastPresence(client, 'online');
      }

      switch (data.type) {
        case 'join':
          return handleJoin(client, data);
        case 'message':
          return handleMessage(client, data);
        case 'private_message':
          return handlePrivateMessage(client, data);
        case 'switch_room':
          return handleSwitchRoom(client, data);
        case 'create_room':
          return handleCreateRoom(client, data);
        case 'typing':
          return handleTyping(client);
        case 'kick':
          return handleModeration(client, data, 'kick');
        case 'mute':
          return handleModeration(client, data, 'mute');
        case 'unmute':
          return handleModeration(client, data, 'unmute');
        default:
          return; // unknown type: ignore rather than crash the connection
      }
    });

    ws.on('close', () => {
      clientsById.delete(clientId);
      if (client.username) usernameToId.delete(client.username.toLowerCase());

      if (client.room) {
        roomManager.leave(client.room, clientId);
        roomManager.broadcast(client.room, {
          type: 'notification',
          text: `${client.username} left #${client.room}`,
          users: roomManager.getUsernames(client.room),
          room: client.room,
        });
      }
      if (client.username) {
        broadcastPresence(client, 'offline');
        logger.log('disconnect', { username: client.username, clientId });
      }
    });

    ws.on('error', (err) => {
      logger.log('socket_error', { clientId, message: err.message });
    });
  });

  // ---------------------------------------------------------
  // Handler: join
  // ---------------------------------------------------------
  function handleJoin(client, data) {
    if (client.username) return; // already joined; ignore repeat joins

    const requested = (data.username || '').toString().trim().slice(0, config.MAX_USERNAME_LEN) || `User${client.id.slice(0, 4)}`;
    const username = uniqueUsername(requested);
    const room = isValidRoomName(data.room) ? data.room : config.DEFAULT_ROOMS[0];

    client.username = username;
    client.room = room;
    client.isAdmin = config.ADMIN_USERNAMES.includes(username.toLowerCase());

    usernameToId.set(username.toLowerCase(), client.id);
    roomManager.join(room, client.id, { ws: client.ws, username });

    const history = store.getHistory(room, config.HISTORY_LIMIT);

    client.ws.send(JSON.stringify({
      type: 'welcome',
      username,
      room,
      isAdmin: client.isAdmin,
      users: roomManager.getUsernames(room),
      rooms: roomManager.listRooms(),
      onlineUsers: Array.from(usernameToId.keys()),
      history,
    }));

    roomManager.broadcast(room, {
      type: 'notification',
      text: `${username} joined #${room}`,
      users: roomManager.getUsernames(room),
      room,
    }, client.id);

    broadcastToAll({ type: 'room_list', rooms: roomManager.listRooms() });
    broadcastPresence(client, 'online');
    logger.log('join', { username, room, clientId: client.id, isAdmin: client.isAdmin });
  }

  // ---------------------------------------------------------
  // Handler: room chat message
  // ---------------------------------------------------------
  function handleMessage(client, data) {
    if (!client.username) return;
    if (client.muted) return sendError(client.ws, 'muted', 'You have been muted by a moderator.');
    if (!client.bucket.tryConsume(1)) {
      return sendError(client.ws, 'rate_limited', 'Sending too fast — slow down a little.');
    }

    const text = (data.text || '').toString().trim().slice(0, config.MAX_MESSAGE_LEN);
    if (!text) return;

    const msg = {
      id: randomUUID(),
      username: client.username,
      text,
      room: client.room,
      timestamp: Date.now(),
    };
    store.appendMessage(client.room, msg);
    roomManager.broadcast(client.room, { type: 'message', ...msg });
    client.ws.send(JSON.stringify({ type: 'delivered', id: msg.id }));
  }

  // ---------------------------------------------------------
  // Handler: private (1-to-1) message
  // ---------------------------------------------------------
  function handlePrivateMessage(client, data) {
    if (!client.username) return;
    if (client.muted) return sendError(client.ws, 'muted', 'You have been muted by a moderator.');
    if (!client.bucket.tryConsume(1)) {
      return sendError(client.ws, 'rate_limited', 'Sending too fast — slow down a little.');
    }

    const targetName = (data.to || '').toString().trim();
    const targetId = usernameToId.get(targetName.toLowerCase());
    if (!targetId) return sendError(client.ws, 'user_offline', `${targetName} is not online.`);

    const text = (data.text || '').toString().trim().slice(0, config.MAX_MESSAGE_LEN);
    if (!text) return;

    const dm = {
      id: randomUUID(),
      from: client.username,
      to: targetName,
      text,
      timestamp: Date.now(),
    };

    // Persisted to a per-pair audit log (not replayed live — kept
    // deliberately simple — but demonstrates the same durable-write
    // path used for room messages, and gives a paper trail).
    const pairKey = [client.username.toLowerCase(), targetName.toLowerCase()].sort().join('__');
    store.appendMessage(`dm-${pairKey}`, dm);

    const targetClient = clientsById.get(targetId);
    if (targetClient) targetClient.ws.send(JSON.stringify({ type: 'private_message', ...dm }));
    client.ws.send(JSON.stringify({ type: 'private_message', ...dm })); // echo to sender
  }

  // ---------------------------------------------------------
  // Handler: switch_room
  // ---------------------------------------------------------
  function handleSwitchRoom(client, data) {
    if (!client.username) return;
    const newRoom = data.room;
    if (!isValidRoomName(newRoom) || !roomManager.roomExists(newRoom)) {
      return sendError(client.ws, 'no_such_room', `Room "${newRoom}" doesn't exist.`);
    }
    if (newRoom === client.room) return;

    const oldRoom = client.room;
    roomManager.leave(oldRoom, client.id);
    roomManager.broadcast(oldRoom, {
      type: 'notification',
      text: `${client.username} left #${oldRoom}`,
      users: roomManager.getUsernames(oldRoom),
      room: oldRoom,
    });

    client.room = newRoom;
    roomManager.join(newRoom, client.id, { ws: client.ws, username: client.username });

    const history = store.getHistory(newRoom, config.HISTORY_LIMIT);
    client.ws.send(JSON.stringify({
      type: 'room_switched',
      room: newRoom,
      users: roomManager.getUsernames(newRoom),
      history,
    }));

    roomManager.broadcast(newRoom, {
      type: 'notification',
      text: `${client.username} joined #${newRoom}`,
      users: roomManager.getUsernames(newRoom),
      room: newRoom,
    }, client.id);

    broadcastToAll({ type: 'room_list', rooms: roomManager.listRooms() });
  }

  // ---------------------------------------------------------
  // Handler: create_room
  // ---------------------------------------------------------
  function handleCreateRoom(client, data) {
    if (!client.username) return;
    const name = (data.room || '').toString().trim();
    if (!isValidRoomName(name)) {
      return sendError(client.ws, 'bad_room_name', 'Room names: 1-24 chars, letters/numbers/_/- only.');
    }
    const isNew = !roomManager.roomExists(name);
    roomManager.ensureRoom(name);
    if (isNew) broadcastToAll({ type: 'room_list', rooms: roomManager.listRooms() });
    handleSwitchRoom(client, { room: name });
  }

  // ---------------------------------------------------------
  // Handler: typing indicator
  // ---------------------------------------------------------
  function handleTyping(client) {
    if (!client.username || !client.room) return;
    roomManager.broadcast(client.room, { type: 'typing', username: client.username, room: client.room }, client.id);
  }

  // ---------------------------------------------------------
  // Handler: moderation (kick / mute / unmute) — admin only
  // ---------------------------------------------------------
  function handleModeration(client, data, action) {
    if (!client.username) return;
    if (!client.isAdmin) return sendError(client.ws, 'not_admin', 'Only moderators can do that.');

    const targetName = (data.target || '').toString().trim();
    const targetId = usernameToId.get(targetName.toLowerCase());
    const target = targetId ? clientsById.get(targetId) : null;
    if (!target) return sendError(client.ws, 'user_offline', `${targetName} is not online.`);

    if (action === 'kick') {
      target.ws.send(JSON.stringify({ type: 'kicked', by: client.username }));
      target.ws.close(4001, 'Kicked by moderator');
      logger.log('moderation_kick', { by: client.username, target: targetName });
    } else if (action === 'mute') {
      target.muted = true;
      if (target.room) {
        roomManager.broadcast(target.room, { type: 'notification', text: `${targetName} was muted by ${client.username}`, users: roomManager.getUsernames(target.room), room: target.room });
      }
      logger.log('moderation_mute', { by: client.username, target: targetName });
    } else if (action === 'unmute') {
      target.muted = false;
      if (target.room) {
        roomManager.broadcast(target.room, { type: 'notification', text: `${targetName} was unmuted by ${client.username}`, users: roomManager.getUsernames(target.room), room: target.room });
      }
      logger.log('moderation_unmute', { by: client.username, target: targetName });
    }
  }

  // ---------------------------------------------------------
  // Background: heartbeat (dead-connection reaping)
  // ---------------------------------------------------------
  const heartbeat = setInterval(() => {
    for (const client of clientsById.values()) {
      if (client.ws.isAlive === false) {
        client.ws.terminate(); // -> 'close' handler does cleanup + leave notice
        continue;
      }
      client.ws.isAlive = false;
      client.ws.ping();
    }
  }, config.HEARTBEAT_INTERVAL_MS);

  // ---------------------------------------------------------
  // Background: presence (idle -> "away") sweep
  // ---------------------------------------------------------
  const presenceSweep = setInterval(() => {
    const now = Date.now();
    for (const client of clientsById.values()) {
      if (!client.username) continue;
      const idleFor = now - client.lastActivity;
      if (idleFor > config.IDLE_TIMEOUT_MS && client.presence !== 'away') {
        client.presence = 'away';
        broadcastPresence(client, 'away');
      }
    }
  }, config.PRESENCE_CHECK_INTERVAL_MS);

  function shutdown() {
    clearInterval(heartbeat);
    clearInterval(presenceSweep);
    broadcastToAll({ type: 'system_shutdown', text: 'Server is shutting down. You will be disconnected.' });
    for (const client of clientsById.values()) client.ws.close(1001, 'Server shutting down');
  }

  return { shutdown, clientsById, usernameToId };
}

module.exports = { attachWsServer };
