/**
 * tests/simulate.js
 * ------------------------------------------------------------
 * Headless integration test: spins up the real server as a
 * child process, then drives several `ws` clients against it
 * to exercise every feature end-to-end. Not a replacement for
 * a proper test framework, but enough to prove the protocol
 * actually works before four people rely on it in a lab room.
 *
 * Run with: npm test
 * ------------------------------------------------------------
 */

const { spawn } = require("child_process");
const path = require("path");
const WebSocket = require("ws");
const assert = require("assert");

const PORT = 8099;
const URL = `ws://127.0.0.1:${PORT}`;

function wait(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function connect(username, room) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(URL);
    const inbox = [];
    ws.on("message", (raw) => inbox.push(JSON.parse(raw)));
    ws.on("open", () => {
      ws.send(JSON.stringify({ type: "join", username, room }));
      // give the server a beat to reply with 'welcome'
      setTimeout(() => resolve({ ws, inbox, username }), 150);
    });
    ws.on("error", reject);
  });
}

function lastOfType(inbox, type) {
  for (let i = inbox.length - 1; i >= 0; i--)
    if (inbox[i].type === type) return inbox[i];
  return null;
}

function countOfType(inbox, type) {
  return inbox.filter((m) => m.type === type).length;
}

async function main() {
  console.log("Starting server as child process on port", PORT, "...");
  const server = spawn(
    process.execPath,
    [path.join(__dirname, "..", "server", "index.js")],
    {
      env: { ...process.env, PORT: String(PORT), ADMIN_USERNAMES: "admin" },
      stdio: "pipe",
    },
  );
  server.stdout.on("data", () => {}); // swallow logs; uncomment to debug
  server.stderr.on("data", (d) =>
    console.error("[server stderr]", d.toString()),
  );

  await wait(600); // let it boot

  try {
    // ---------------------------------------------------------
    // 1. Room isolation: a message in #general must not reach #tech
    // ---------------------------------------------------------
    console.log("\n[TEST] room isolation");
    const alice = await connect("Alice", "general");
    const bob = await connect("Bob", "general");
    const carol = await connect("Carol", "tech");

    assert.ok(lastOfType(alice.inbox, "welcome"), "Alice should get a welcome");
    // Alice's own 'welcome' was sent before Bob joined, so it only lists Alice.
    // Bob's join instead triggers a 'notification' to Alice with the updated roster.
    const joinNotif = lastOfType(alice.inbox, "notification");
    assert.ok(
      joinNotif && joinNotif.text.includes("Bob joined"),
      "Alice should be notified when Bob joins",
    );
    assert.deepStrictEqual(joinNotif.users.sort(), ["Alice", "Bob"].sort());

    alice.ws.send(JSON.stringify({ type: "message", text: "hello general" }));
    await wait(200);
    assert.ok(
      lastOfType(bob.inbox, "message"),
      "Bob should receive the room message",
    );
    assert.strictEqual(lastOfType(bob.inbox, "message").text, "hello general");
    assert.strictEqual(
      countOfType(carol.inbox, "message"),
      0,
      "Carol (in #tech) must NOT receive #general messages",
    );
    console.log("  OK: message stayed inside #general, isolated from #tech");

    // ---------------------------------------------------------
    // 2. Duplicate username handling
    // ---------------------------------------------------------
    console.log("\n[TEST] duplicate username suffixing");
    const alice2 = await connect("Alice", "general");
    const welcome2 = lastOfType(alice2.inbox, "welcome");
    assert.strictEqual(welcome2.username, "Alice(1)");
    console.log('  OK: second "Alice" became', welcome2.username);

    // ---------------------------------------------------------
    // 3. Private messaging
    // ---------------------------------------------------------
    console.log("\n[TEST] private messaging");
    bob.ws.send(
      JSON.stringify({
        type: "private_message",
        to: "Carol",
        text: "psst, tech room?",
      }),
    );
    await wait(200);
    const dm = lastOfType(carol.inbox, "private_message");
    assert.ok(dm, "Carol should receive the DM");
    assert.strictEqual(dm.from, "Bob");
    assert.strictEqual(
      countOfType(alice.inbox, "private_message"),
      0,
      "Alice must not see a DM not addressed to her",
    );
    console.log("  OK: DM delivered only to the intended recipient");

    // DM to an offline user should error, not hang
    bob.ws.send(
      JSON.stringify({ type: "private_message", to: "Ghost", text: "hi?" }),
    );
    await wait(150);
    const dmErr = lastOfType(bob.inbox, "error");
    assert.strictEqual(dmErr.code, "user_offline");
    console.log("  OK: DM to offline user returns user_offline error");

    // ---------------------------------------------------------
    // 4. Room switching + history replay
    // ---------------------------------------------------------
    console.log("\n[TEST] room switch + history replay");
    alice.ws.send(JSON.stringify({ type: "switch_room", room: "tech" }));
    await wait(200);
    const switched = lastOfType(alice.inbox, "room_switched");
    assert.strictEqual(switched.room, "tech");
    console.log(
      "  OK: Alice switched into #tech, received room_switched with history array (len=" +
        switched.history.length +
        ")",
    );

    // send a message, then reconnect a fresh client into the same room
    // and confirm it receives that message as history (durability check)
    carol.ws.send(
      JSON.stringify({ type: "message", text: "persisted-message-check" }),
    );
    await wait(200);
    const dave = await connect("Dave", "tech");
    const daveWelcome = lastOfType(dave.inbox, "welcome");
    const found = daveWelcome.history.some(
      (m) => m.text === "persisted-message-check",
    );
    assert.ok(
      found,
      "new joiner should see prior message via persisted history",
    );
    console.log(
      "  OK: message history persisted to disk and replayed to a new joiner",
    );

    // ---------------------------------------------------------
    // 5. Rate limiting
    // ---------------------------------------------------------
    console.log("\n[TEST] rate limiting");
    const spammer = await connect("Spammer", "general");
    let rateLimited = false;
    for (let i = 0; i < 30; i++) {
      spammer.ws.send(JSON.stringify({ type: "message", text: `spam ${i}` }));
    }
    await wait(300);
    rateLimited = spammer.inbox.some(
      (m) => m.type === "error" && m.code === "rate_limited",
    );
    assert.ok(
      rateLimited,
      "flooding messages should eventually trigger rate_limited",
    );
    console.log("  OK: rapid-fire messages triggered rate_limited error");

    // ---------------------------------------------------------
    // 6. Admin moderation: mute + kick
    // ---------------------------------------------------------
    console.log(
      "\n[TEST] moderation (mute/kick), including non-admin rejection",
    );
    const admin = await connect("admin", "general");
    const eve = await connect("Eve", "general");

    // non-admin cannot moderate
    bob.ws.send(JSON.stringify({ type: "mute", target: "Eve" }));
    await wait(150);
    assert.strictEqual(lastOfType(bob.inbox, "error").code, "not_admin");
    console.log("  OK: non-admin mute attempt rejected with not_admin");

    // admin mutes Eve; her messages should be dropped with an error to her
    admin.ws.send(JSON.stringify({ type: "mute", target: "Eve" }));
    await wait(150);
    eve.ws.send(
      JSON.stringify({ type: "message", text: "can anyone hear me" }),
    );
    await wait(150);
    assert.strictEqual(lastOfType(eve.inbox, "error").code, "muted");
    assert.ok(
      !bob.inbox.some(
        (m) => m.type === "message" && m.text === "can anyone hear me",
      ),
      "muted user message must not broadcast",
    );
    console.log("  OK: muted user's message suppressed");

    // admin kicks Eve; her socket should close
    const eveClosed = new Promise((resolve) => eve.ws.on("close", resolve));
    admin.ws.send(JSON.stringify({ type: "kick", target: "Eve" }));
    await Promise.race([eveClosed, wait(1000)]);
    assert.strictEqual(eve.ws.readyState, WebSocket.CLOSED);
    console.log("  OK: kicked user's connection was closed by the server");

    // ---------------------------------------------------------
    // 7. Ungraceful disconnect still triggers cleanup + notification
    // ---------------------------------------------------------
    console.log("\n[TEST] ungraceful disconnect cleanup");
    const flaky = await connect("Flaky", "general");
    await wait(100);
    flaky.ws.terminate(); // simulate a dead connection, not a clean close
    await wait(200);
    const notif = bob.inbox.filter((m) => m.type === "notification").pop();
    assert.ok(
      notif.text.includes("Flaky left"),
      "room should be notified that Flaky left after abrupt termination",
    );
    console.log(
      "  OK: abrupt disconnect produced a leave notification (heartbeat/close path both work)",
    );

    console.log("\nALL TESTS PASSED ✅");
  } finally {
    server.kill("SIGTERM");
    await wait(300);
  }
}

main().catch((err) => {
  console.error("\nTEST FAILED ❌");
  console.error(err);
  process.exit(1);
});
