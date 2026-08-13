/**
 * config.js
 * ------------------------------------------------------------
 * Single source of truth for tunables. Keeping this separate
 * (instead of magic numbers scattered through the codebase) is
 * a basic separation-of-concerns move: anyone tuning behavior
 * for a demo/lab reads one file, not five.
 *
 * Everything here can be overridden with environment variables
 * so the same code runs unmodified on any lab machine.
 * ------------------------------------------------------------
 */

function csv(name, fallback) {
  const raw = process.env[name];
  if (!raw) return fallback;
  return raw.split(',').map((s) => s.trim()).filter(Boolean);
}

module.exports = {
  PORT: parseInt(process.env.PORT || '8080', 10),

  // Rooms that always exist, even with nobody in them.
  DEFAULT_ROOMS: csv('DEFAULT_ROOMS', ['general', 'random', 'tech']),

  // Usernames (case-insensitive) granted moderator powers (kick/mute).
  // Set ADMIN_USERNAMES="alice,bob" as an env var to change this per-run.
  ADMIN_USERNAMES: csv('ADMIN_USERNAMES', ['admin']).map((s) => s.toLowerCase()),

  // How many past messages are replayed to a client when it joins/switches a room.
  HISTORY_LIMIT: parseInt(process.env.HISTORY_LIMIT || '50', 10),

  // Dead-connection detection: ping every N ms, drop clients that never pong.
  HEARTBEAT_INTERVAL_MS: parseInt(process.env.HEARTBEAT_INTERVAL_MS || '30000', 10),

  // Presence: mark a user "away" after this much inactivity.
  IDLE_TIMEOUT_MS: parseInt(process.env.IDLE_TIMEOUT_MS || '60000', 10),
  PRESENCE_CHECK_INTERVAL_MS: 15000,

  // Anti-spam token bucket: BURST tokens available immediately,
  // refilling at REFILL_PER_SEC tokens/second thereafter.
  RATE_LIMIT: {
    BURST: parseInt(process.env.RATE_LIMIT_BURST || '8', 10),
    REFILL_PER_SEC: parseFloat(process.env.RATE_LIMIT_REFILL || '2'),
  },

  MAX_USERNAME_LEN: 20,
  MAX_MESSAGE_LEN: 1000,
  MAX_ROOM_NAME_LEN: 24,
};
