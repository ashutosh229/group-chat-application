/**
 * logger.js
 * ------------------------------------------------------------
 * Minimal structured logger. Every line is a single JSON object
 * with a timestamp and event name, instead of ad-hoc console.log
 * strings. This is a small nod to real operational practice:
 * structured logs are grep/jq-able and can be piped into a log
 * aggregator later without changing call sites.
 * ------------------------------------------------------------
 */

function log(event, meta = {}) {
  const line = { ts: new Date().toISOString(), event, ...meta };
  console.log(JSON.stringify(line));
}

module.exports = { log };
