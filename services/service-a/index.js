// app.js
import express from "express";
import amqp from "amqplib";
import crypto from "node:crypto";
import client from "prom-client";
import { expressjwt } from "express-jwt";
import jwksRsa from "jwks-rsa";

const PORT = process.env.PORT || 3000;
const RABBITMQ_URL = process.env.RABBITMQ_URL;
const QUEUE_NAME = process.env.QUEUE_NAME || "events.demo";
const KEYCLOAK_ISSUER = process.env.KEYCLOAK_ISSUER;
const JWKS_URI = process.env.JWKS_URI;

// ---- Build / Info metadata (optionnel mais très utile en prod)
const SERVICE_NAME = process.env.SERVICE_NAME ?? "service-a";
const VERSION = process.env.VERSION ?? "0.0.0";
const COMMIT = process.env.COMMIT ?? "dev";
const BUILD_TIME = process.env.BUILD_TIME ?? new Date().toISOString();
const ENV = process.env.NODE_ENV ?? "development";

const app = express();
app.use(express.json());

// ---- Pretty console logs (JSON) + requestId + duration
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

  const start = process.hrtime.bigint();

  res.setHeader("x-request-id", requestId);

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

// ---- JWT check (Keycloak JWKS)
const jwtCheck = expressjwt({
  secret: jwksRsa.expressJwtSecret({
    cache: true,
    jwksUri: JWKS_URI,
  }),
  audience: undefined, // simplifié (client public)
  issuer: KEYCLOAK_ISSUER,
  algorithms: ["RS256"],
});

// ---- RabbitMQ
let channel;
let rabbitConn;

async function initRabbitWithRetry(maxRetries = 30, delayMs = 1000) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      rabbitConn = await amqp.connect(RABBITMQ_URL);
      channel = await rabbitConn.createChannel();
      await channel.assertQueue(QUEUE_NAME, { durable: true });

      log("info", "rabbitmq_connected", { attempt, queue: QUEUE_NAME });
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

// Ne pas mesurer les endpoints techniques (sinon bruit énorme)
const TECH_PATHS = new Set(["/live", "/ready", "/health", "/metrics", "/info"]);

app.use((req, res, next) => {
  const start = process.hrtime.bigint();

  res.on("finish", () => {
    if (TECH_PATHS.has(req.path)) return;

    const end = process.hrtime.bigint();
    const durationSeconds = Number(end - start) / 1e9;

    // route "logique" (évite /users/123 => /users/:id)
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

// ---- Dependency check (utilisé par /ready & /health)
async function checkDependencies() {
  // Ici on vérifie RabbitMQ. (Tu peux ajouter d'autres checks si besoin)
  if (!isRabbitReady()) {
    return { ok: false, rabbitmq: "DOWN" };
  }
  return { ok: true, rabbitmq: "UP" };
}

// --------------------
// ENDPOINTS TECHNIQUES
// --------------------

// /live : le process est vivant (ne dépend pas de Rabbit/DB)
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

// /health : état global (souvent similaire à /ready, peut inclure plus d’infos)
app.get("/health", async (req, res) => {
  const deps = await checkDependencies();
  const payload = {
    status: deps.ok ? "UP" : "DOWN",
    service: SERVICE_NAME,
    rabbitmq: deps.rabbitmq,
    time: new Date().toISOString(),
  };
  res.status(deps.ok ? 200 : 503).json(payload);
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

// /metrics : exposer les métriques Prometheus (texte)
app.get("/metrics", async (req, res) => {
  res.set("Content-Type", client.register.contentType);
  res.end(await client.register.metrics());
});

// --------------------
// ROUTES METIER
// --------------------

// route protégée: publie un event
app.post("/action", jwtCheck, async (req, res) => {
  if (!isRabbitReady()) {
    log("warn", "publish_failed_rabbit_not_ready", { requestId: req.requestId });
    return res.status(503).json({ ok: false, error: "RabbitMQ not ready" });
  }

  const event = {
    eventType: "ActionRequested",
    id: crypto.randomUUID(),
    timestamp: new Date().toISOString(),
    actor: req.auth?.preferred_username ?? "unknown",
    payload: req.body ?? {},
  };

  try {
    channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(event)), {
      persistent: true,
    });

    log("info", "event_published", {
      requestId: req.requestId,
      queue: QUEUE_NAME,
      eventType: event.eventType,
      eventId: event.id,
      actor: event.actor,
    });

    res.json({ ok: true, published: event });
  } catch (e) {
    log("error", "publish_failed", {
      requestId: req.requestId,
      error: e?.message ?? String(e),
    });
    res.status(500).json({ ok: false, error: "Publish failed" });
  }
});

// ---- Start
initRabbitWithRetry()
  .then(() => {
    app.listen(PORT, () => {
      log("info", "service_started", {
        port: PORT,
        queue: QUEUE_NAME,
        issuer: KEYCLOAK_ISSUER,
        jwksUri: JWKS_URI,
      });
    });
  })
  .catch((e) => {
    log("error", "rabbit_init_failed", { error: e?.message ?? String(e) });
    process.exit(1);
  });

// ---- Graceful shutdown (optionnel mais propre)
process.on("SIGTERM", async () => {
  log("info", "sigterm_received");
  try {
    if (channel) await channel.close();
    if (rabbitConn) await rabbitConn.close();
  } catch (e) {
    log("warn", "shutdown_error", { error: e?.message ?? String(e) });
  } finally {
    process.exit(0);
  }
});
