import express from "express";
import amqp from "amqplib";
import { expressjwt } from "express-jwt";
import jwksRsa from "jwks-rsa";

const PORT = process.env.PORT || 3000;
const RABBITMQ_URL = process.env.RABBITMQ_URL;
const QUEUE_NAME = process.env.QUEUE_NAME || "events.demo";
const KEYCLOAK_ISSUER = process.env.KEYCLOAK_ISSUER;
const JWKS_URI = process.env.JWKS_URI;

const app = express();
app.use(express.json());

const jwtCheck = expressjwt({
  secret: jwksRsa.expressJwtSecret({
    cache: true,
    jwksUri: JWKS_URI
  }),
  audience: undefined, // simplifié (client public)
  issuer: KEYCLOAK_ISSUER,
  algorithms: ["RS256"]
});

let channel;

async function initRabbitWithRetry(maxRetries = 30, delayMs = 1000) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const conn = await amqp.connect(RABBITMQ_URL);
      channel = await conn.createChannel();
      await channel.assertQueue(QUEUE_NAME, { durable: true });
      console.log(`[service-a] RabbitMQ connected (attempt ${attempt})`);
      return;
    } catch (e) {
      console.warn(`[service-a] RabbitMQ not ready (attempt ${attempt}/${maxRetries})`);
      if (attempt === maxRetries) throw e;
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
}

<<<<<<< HEAD


app.get("/health", async (req, res) => {
=======
app.get("/api/a/health", async (req, res) => {
>>>>>>> 9eb1e77149fd627845fc26edc0703581ceb25f07
  res.json({
    status: "UP",
    service: "service-a",
    rabbitmq: channel ? "UP" : "DOWN",
    time: new Date().toISOString()
  });
});

// route protégée: publie un event
app.post("/api/a/action", jwtCheck, async (req, res) => {
  const event = {
    eventType: "ActionRequested",
    id: crypto.randomUUID(),
    timestamp: new Date().toISOString(),
    actor: req.auth?.preferred_username ?? "unknown",
    payload: req.body ?? {}
  };

  channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(event)), { persistent: true });
  res.json({ ok: true, published: event });
});

initRabbitWithRetry()
  .then(() => app.listen(PORT, () => console.log(`service-a on ${PORT}`)))
  .catch((e) => {
    console.error("Rabbit init failed", e);
    process.exit(1);
  });
