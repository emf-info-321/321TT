// app.js
import express from "express";
import amqp from "amqplib";
import pg from "pg";
import crypto from "node:crypto";
import client from "prom-client";

const PORT = process.env.PORT || 3001;
const RABBITMQ_URL = process.env.RABBITMQ_URL;
const QUEUE_NAME = process.env.QUEUE_NAME || "events.demo";
const DATABASE_URL = process.env.DATABASE_URL;

// ---- Build / Info metadata
const SERVICE_NAME = process.env.SERVICE_NAME ?? "service-b";
const VERSION = process.env.VERSION ?? "0.0.0";
const COMMIT = process.env.COMMIT ?? "dev";
const BUILD_TIME = process.env.BUILD_TIME ?? new Date().toISOString();
const ENV = process.env.NODE_ENV ?? "development";

const app = express();
app.use(express.json());

const pool = new pg.Pool({ connectionString: DATABASE_URL });

// ---- Pretty logs (JSON) + requestId + duration
function log(level, message, extra = {}) {
  console.log(
    JSON.stringify({
      ts: new Date().toISOString(),
      level,
      service: SERVICE_NAME,
      message,
      ...extra,
    })
  );
}

app.use((req, res, next) => {
  const requestId = req.header("x-request-id") || crypto.randomUUID();
  req.requestId = requestId;
  res.setHeader("x-request-id", requestId);

  const start = process.hrtime.bigint();

  res.on("finish", () => {
    const end = process.hrtime.bigint();
    const durationMs = Number(end - start) / 1e6;

    log("info", "request", {
      requestId,
      method: req.method,
      path: req.path,
      statusCode: res.statusCode,
      durationMs: Math.round(durationMs * 100) / 100,
    });
  });

  next();
});

// ---- Metrics (Prometheus)
client.collectDefaultMetrics();

const httpRequestsTotal = new client.Counter({
  name: "http_requests_total",
  help: "Total number of HTTP requests",
  labelNames: ["method", "route", "status_code"],
});

const httpRequestDuration = new client.Histogram({
  name: "http_request_duration_seconds",
  help: "Duration of HTTP requests in seconds",
  labelNames: ["method", "route", "status_code"],
  buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5],
});

// Consumer metrics (utile pour un worker/consumer)
const rabbitMessagesConsumedTotal = new client.Counter({
  name: "rabbitmq_messages_consumed_total",
  help: "Total messages consumed from RabbitMQ",
  labelNames: ["queue"],
});

const rabbitMessagesFailedTotal = new client.Counter({
  name: "rabbitmq_messages_failed_total",
  help: "Total messages failed (nack/drop) from RabbitMQ",
  labelNames: ["queue"],
});

const dbInsertsTotal = new client.Counter({
  name: "db_inserts_total",
  help: "Total DB inserts attempted",
});

const dbInsertFailuresTotal = new client.Counter({
  name: "db_insert_failures_total",
  help: "Total DB insert failures",
});

const TECH_PATHS = new Set(["/live", "/ready", "/health", "/metrics", "/info"]);

app.use((req, res, next) => {
  const start = process.hrtime.bigint();

  res.on("finish", () => {
    if (TECH_PATHS.has(req.path)) return;

    const end = process.hrtime.bigint();
    const durationSeconds = Number(end - start) / 1e9;

    const route = req.route?.path
      ? req.route.path
      : res.statusCode === 404
        ? "not_found"
        : req.path;

    const labels = {
      method: req.method,
      route,
      status_code: String(res.statusCode),
    };

    httpRequestsTotal.inc(labels, 1);
    httpRequestDuration.observe(labels, durationSeconds);
  });

  next();
});

// ---- RabbitMQ + DB state
let channel;
let rabbitConn;

// ---- DB init
async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS events (
      id TEXT PRIMARY KEY,
      event_type TEXT NOT NULL,
      ts TIMESTAMPTZ NOT NULL,
      actor TEXT,
      payload JSONB NOT NULL
    );
  `);
  log("info", "db_initialized");
}

async function initRabbitWithRetry(maxRetries = 30, delayMs = 1000) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      rabbitConn = await amqp.connect(RABBITMQ_URL);
      channel = await rabbitConn.createChannel();
      await channel.assertQueue(QUEUE_NAME, { durable: true });

      log("info", "rabbitmq_connected", { attempt, queue: QUEUE_NAME });

      channel.consume(QUEUE_NAME, async (msg) => {
        if (!msg) return;

        rabbitMessagesConsumedTotal.inc({ queue: QUEUE_NAME }, 1);

        let event;
        try {
          event = JSON.parse(msg.content.toString());
        } catch (e) {
          // Message illisible -> on drop (pas de requeue)
          rabbitMessagesFailedTotal.inc({ queue: QUEUE_NAME }, 1);
          log("error", "message_parse_failed", { error: e?.message ?? String(e) });
          channel.nack(msg, false, false);
          return;
        }

        dbInsertsTotal.inc(1);

        try {
          await pool.query(
            "INSERT INTO events (id, event_type, ts, actor, payload) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (id) DO NOTHING",
            [event.id, event.eventType, event.timestamp, event.actor ?? null, event.payload ?? {}]
          );

          channel.ack(msg);

          log("info", "event_persisted", {
            queue: QUEUE_NAME,
            eventType: event.eventType,
            eventId: event.id,
          });
        } catch (e) {
          dbInsertFailuresTotal.inc(1);
          rabbitMessagesFailedTotal.inc({ queue: QUEUE_NAME }, 1);

          log("error", "db_insert_failed", {
            error: e?.message ?? String(e),
            eventId: event?.id,
            eventType: event?.eventType,
          });

          // Ici tu as choisi: nack sans requeue (drop) pour éviter boucle infinie
          channel.nack(msg, false, false);
        }
      });

      return;
    } catch (e) {
      log("warn", "rabbitmq_not_ready", {
        attempt,
        maxRetries,
        error: e?.message ?? String(e),
      });

      if (attempt === maxRetries) throw e;
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
}

function isRabbitReady() {
  return Boolean(channel);
}

async function isDbReady() {
  try {
    await pool.query("SELECT 1");
    return true;
  } catch {
    return false;
  }
}

// ---- Dependency check for /ready & /health
async function checkDependencies() {
  const dbOk = await isDbReady();
  const rabbitOk = isRabbitReady();

  return {
    ok: dbOk && rabbitOk,
    db: dbOk ? "UP" : "DOWN",
    rabbitmq: rabbitOk ? "UP" : "DOWN",
  };
}

// --------------------
// ENDPOINTS TECHNIQUES
// --------------------

// /live : le process est vivant (ne dépend pas de DB/Rabbit)
app.get("/live", (req, res) => {
  res.json({ status: "alive" });
});

// /ready : prêt à recevoir du trafic (dépendances OK)
app.get("/ready", async (req, res) => {
  const deps = await checkDependencies();

  if (!deps.ok) {
    return res.status(503).json({
      status: "not_ready",
      service: SERVICE_NAME,
      ...deps,
      time: new Date().toISOString(),
    });
  }

  res.json({
    status: "ready",
    service: SERVICE_NAME,
    ...deps,
    time: new Date().toISOString(),
  });
});

// /health : état global
app.get("/health", async (req, res) => {
  const deps = await checkDependencies();

  res.status(deps.ok ? 200 : 503).json({
    status: deps.ok ? "UP" : "DOWN",
    service: SERVICE_NAME,
    db: deps.db,
    rabbitmq: deps.rabbitmq,
    time: new Date().toISOString(),
  });
});

// /info : version/build/commit/env
app.get("/info", (req, res) => {
  res.json({
    service: SERVICE_NAME,
    version: VERSION,
    commit: COMMIT,
    buildTime: BUILD_TIME,
    environment: ENV,
  });
});

// /metrics : format Prometheus (text/plain)
app.get("/metrics", async (req, res) => {
  res.set("Content-Type", client.register.contentType);
  res.end(await client.register.metrics());
});

// --------------------
// ROUTES METIER
// --------------------

app.get("/events", async (req, res) => {
  const r = await pool.query("SELECT * FROM events ORDER BY ts DESC LIMIT 50");
  res.json(r.rows);
});

// ---- Start
Promise.all([initDb(), initRabbitWithRetry()])
  .then(() => {
    app.listen(PORT, () => {
      log("info", "service_started", {
        port: PORT,
        queue: QUEUE_NAME,
        databaseUrlPresent: Boolean(DATABASE_URL),
        rabbitUrlPresent: Boolean(RABBITMQ_URL),
      });
    });
  })
  .catch((e) => {
    log("error", "init_failed", { error: e?.message ?? String(e) });
    process.exit(1);
  });

// ---- Graceful shutdown (propre)
process.on("SIGTERM", async () => {
  log("info", "sigterm_received");
  try {
    if (channel) await channel.close();
    if (rabbitConn) await rabbitConn.close();
    await pool.end();
  } catch (e) {
    log("warn", "shutdown_error", { error: e?.message ?? String(e) });
  } finally {
    process.exit(0);
  }
});
