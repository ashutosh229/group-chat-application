/**
 * index.js
 * ------------------------------------------------------------
 * Entry point. Responsibilities kept deliberately narrow:
 *   1. Serve the static frontend (public/) over HTTP.
 *   2. Attach a WebSocket server to the same HTTP port (one
 *      port to open on the lab firewall, not two).
 *   3. Wire RoomManager + wsServer together.
 *   4. Handle process-level signals for a clean shutdown.
 *
 * All actual protocol/business logic lives in wsServer.js,
 * rooms.js, store.js, etc. — this file just assembles them.
 * ------------------------------------------------------------
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const config = require('./config');
const logger = require('./logger');
const { RoomManager } = require('./rooms');
const { attachWsServer } = require('./wsServer');

const PUBLIC_DIR = path.join(__dirname, '..', 'public');
const MIME_TYPES = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css' };

// ---------------------------------------------------------------
// Static file server for the frontend
// ---------------------------------------------------------------
const httpServer = http.createServer((req, res) => {
  let filePath = req.url === '/' ? '/index.html' : req.url;
  filePath = path.join(PUBLIC_DIR, filePath);

  if (!filePath.startsWith(PUBLIC_DIR)) {
    res.writeHead(403);
    return res.end('Forbidden');
  }

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      return res.end('404 Not Found');
    }
    const ext = path.extname(filePath);
    res.writeHead(200, { 'Content-Type': MIME_TYPES[ext] || 'application/octet-stream' });
    res.end(data);
  });
});

// ---------------------------------------------------------------
// WebSocket server + protocol wiring
// ---------------------------------------------------------------
const wss = new WebSocket.Server({ server: httpServer });
const roomManager = new RoomManager(logger);
const { shutdown } = attachWsServer(wss, roomManager, logger);

// ---------------------------------------------------------------
// Graceful process shutdown: notify clients, close sockets, exit.
// ---------------------------------------------------------------
function gracefulExit(signal) {
  logger.log('server_shutdown', { signal });
  shutdown();
  httpServer.close(() => process.exit(0));
  // Force-exit if sockets don't close within 3s.
  setTimeout(() => process.exit(0), 3000).unref();
}
process.on('SIGINT', () => gracefulExit('SIGINT'));
process.on('SIGTERM', () => gracefulExit('SIGTERM'));

httpServer.listen(config.PORT, '0.0.0.0', () => {
  logger.log('server_start', {
    port: config.PORT,
    rooms: config.DEFAULT_ROOMS,
    admins: config.ADMIN_USERNAMES,
  });
  console.log(`\nGroup Chat server running:`);
  console.log(`  Local:   http://localhost:${config.PORT}`);
  console.log(`  Network: http://<this-machine-IP>:${config.PORT}  (use for other lab machines)`);
  console.log(`  Admins:  ${config.ADMIN_USERNAMES.join(', ')} (set ADMIN_USERNAMES env var to change)\n`);
});
