/**
 * rooms.js
 * ------------------------------------------------------------
 * Tracks chat rooms and who is currently in each one.
 *
 * The baseline tutorial had exactly one implicit "room" (the
 * whole server). Real chat apps isolate broadcast scope by
 * room/channel, so a message sent in #tech never reaches
 * someone sitting in #random. RoomManager is the component
 * responsible for that isolation: it owns the room -> members
 * mapping and is the only thing allowed to broadcast into a room.
 * ------------------------------------------------------------
 */

const WebSocket = require('ws');

class RoomManager {
  constructor(logger) {
    this.logger = logger;
    /** @type {Map<string, {name:string, members:Map<string,{ws:any, username:string}>, createdAt:number}>} */
    this.rooms = new Map();
  }

  ensureRoom(name) {
    if (!this.rooms.has(name)) {
      this.rooms.set(name, { name, members: new Map(), createdAt: Date.now() });
      this.logger.log('room_created', { room: name });
    }
    return this.rooms.get(name);
  }

  roomExists(name) {
    return this.rooms.has(name);
  }

  listRooms() {
    return Array.from(this.rooms.values())
      .map((r) => ({ name: r.name, memberCount: r.members.size }))
      .sort((a, b) => a.name.localeCompare(b.name));
  }

  join(roomName, clientId, member) {
    const room = this.ensureRoom(roomName);
    room.members.set(clientId, member);
  }

  leave(roomName, clientId) {
    const room = this.rooms.get(roomName);
    if (room) room.members.delete(clientId);
  }

  getMembers(roomName) {
    const room = this.rooms.get(roomName);
    return room ? Array.from(room.members.values()) : [];
  }

  getUsernames(roomName) {
    return this.getMembers(roomName).map((m) => m.username);
  }

  /** Send `obj` to every socket in `roomName`, optionally skipping one client. */
  broadcast(roomName, obj, excludeClientId = null) {
    const room = this.rooms.get(roomName);
    if (!room) return;
    const payload = JSON.stringify(obj);
    for (const [id, member] of room.members) {
      if (id === excludeClientId) continue;
      if (member.ws.readyState === WebSocket.OPEN) member.ws.send(payload);
    }
  }
}

module.exports = { RoomManager };
