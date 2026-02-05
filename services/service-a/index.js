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

async function initRabbit() {
  const conn = await amqp.connect(RABBITMQ_URL);
  channel = await conn.createChannel();
  await channel.assertQueue(QUEUE_NAME, { durable: true });
}

app.get("/health", async (req, res) => {
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

initRabbit()
  .then(() => app.listen(PORT, () => console.log(`service-a on ${PORT}`)))
  .catch((e) => {
    console.error("Rabbit init failed", e);
    process.exit(1);
  });
