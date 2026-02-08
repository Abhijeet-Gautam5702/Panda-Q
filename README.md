# Panda-Q 🐼

A single-node, performant & durable message broker with **at-least-once** delivery semantics.

---

## Quick Start

### 1. Clone & Install

```bash
git clone https://github.com/Abhijeet-Gautam5702/Panda-Q.git
cd Panda-Q
npm install
```

### 2. Configure

Edit `pandaq-config.json` to define your topics:

```json
{
  "brokerId": "broker_1",
  "reboot": false,
  "topics": [
    { "id": "my-topic", "partitions": 2 }
  ]
}
```

> **Tip:** Set `"reboot": true` to reset all data on next start.

### 3. Start the Broker

```bash
npm run dev
```

You should see:
```
[SERVER] Panda-Q HTTP Server listening on port 3000
```

### 4. Test the Flow

**Produce a message:**
```bash
curl -X POST http://localhost:3000/ingress/my-topic \
  -H "Content-Type: application/json" \
  -d '{"brokerId": "broker_1", "message": {"messageId": "msg-001", "content": "Hello World"}}'
```

**Consume messages:**
```bash
curl http://localhost:3000/consume/broker_1/my-topic/0
```

**Commit offset:**
```bash
curl -X POST http://localhost:3000/commit \
  -H "Content-Type: application/json" \
  -d '{"brokerId": "broker_1", "topicId": "my-topic", "partitionId": 0, "consumerId": "test", "offset": 1}'
```

---

## Learn More

- 📐 **[Architecture Guide](./ARCHITECTURE.md)** — Deep dive into components, data flow & offset semantics

---

## License

MIT © [Abhijeet Gautam](https://github.com/Abhijeet-Gautam5702)
