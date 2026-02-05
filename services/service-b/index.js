import express from "express";
import amqp from "amqplib";
import pg from "pg";

const PORT = process.env.PORT || 3001;
const RABBITMQ_URL = process.env.RABBITMQ_URL;
const QUEUE_NAME = process.env.QUEUE_NAME || "events.demo";
const DATABASE_URL = process.env.DATABASE_URL;

const app = express();
const pool = new pg.Pool({ connectionString: DATABASE_URL });

let channel;

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
}

async function initRabbitWithRetry(maxRetries = 30, delayMs = 1000) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const conn = await amqp.connect(RABBITMQ_URL);
      channel = await conn.createChannel();
      await channel.assertQueue(QUEUE_NAME, { durable: true });

      channel.consume(QUEUE_NAME, async (msg) => {
        if (!msg) return;
        const event = JSON.parse(msg.content.toString());

        try {
          await pool.query(
            "INSERT INTO events (id, event_type, ts, actor, payload) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (id) DO NOTHING",
            [event.id, event.eventType, event.timestamp, event.actor ?? null, event.payload ?? {}]
          );
          channel.ack(msg);
        } catch (e) {
          console.error("DB insert failed", e);
          channel.nack(msg, false, false);
        }
      });

      console.log(`[service-b] RabbitMQ connected (attempt ${attempt})`);
      return;
    } catch (e) {
      console.warn(`[service-b] RabbitMQ not ready (attempt ${attempt}/${maxRetries})`);
      if (attempt === maxRetries) throw e;
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
}


app.get("/health", async (req, res) => {
  let db = "DOWN";
  try {
    await pool.query("SELECT 1");
    db = "UP";
  } catch {}
  res.json({ status: "UP", service: "service-b", db, rabbitmq: channel ? "UP" : "DOWN", time: new Date().toISOString() });
});

app.get("/events", async (req, res) => {
  const r = await pool.query("SELECT * FROM events ORDER BY ts DESC LIMIT 50");
  res.json(r.rows);
});

Promise.all([initDb(), initRabbitWithRetry()])
  .then(() => app.listen(PORT, () => console.log(`service-b on ${PORT}`)))
  .catch((e) => {
    console.error("Init failed", e);
    process.exit(1);
  });
