/**
 * store.js
 * ------------------------------------------------------------
 * Persistence layer for chat history.
 *
 * Design choice: an append-only JSON-Lines file per room
 * (`data/rooms/<room>.log`), one message per line. This gives us:
 *   - Durability across server restarts (the baseline tutorial
 *     lost all history the moment the process died).
 *   - A write path that's just an fs.appendFileSync — no schema
 *     migrations, no native DB driver, works on any lab machine
 *     with plain Node.js.
 *   - A trivial "replay last N messages" read path for history-
 *     on-join.
 *
 * This module is intentionally the ONLY place that knows about
 * the on-disk format. Everything else in the app calls
 * appendMessage()/getHistory() and doesn't care how or where
 * messages are stored — so swapping this for SQLite/Postgres
 * later only means rewriting this one file.
 * ------------------------------------------------------------
 */

const fs = require('fs');
const path = require('path');

const DATA_DIR = path.join(__dirname, '..', 'data', 'rooms');

function sanitizeName(name) {
  // Keep filenames predictable and traversal-safe regardless of
  // what a client sends as a room name.
  return String(name).replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 64) || 'room';
}

function ensureDir() {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

function roomFile(room) {
  return path.join(DATA_DIR, `${sanitizeName(room)}.log`);
}

/** Append one message object to a room's durable log. */
function appendMessage(room, message) {
  ensureDir();
  fs.appendFileSync(roomFile(room), JSON.stringify(message) + '\n');
}

/** Return up to `limit` most recent messages for a room, oldest first. */
function getHistory(room, limit) {
  ensureDir();
  const file = roomFile(room);
  if (!fs.existsSync(file)) return [];
  const lines = fs.readFileSync(file, 'utf8').trim().split('\n').filter(Boolean);
  const tail = lines.slice(-limit);
  return tail
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

module.exports = { appendMessage, getHistory };
