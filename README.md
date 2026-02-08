![Panda-Q Banner](https://drive.google.com/uc?export=view&id=1amOdDh9TeyEYaMfAlUhViOpC7ADDehEX)
# Panda-Q 🐼

A single-node, performant & durable message broker with **at-least-once** delivery semantics.


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
npm run demo
```

You should see:
```
[SERVER] Panda-Q HTTP Server listening on port 3000
```

### 4. Test with Producer & Consumer

> **IMPORTANT**\
> You need **3 separate terminals** to test the full flow:
> 1. **Terminal 1:** Run `npm run demo` (starts the broker)
> 2. **Terminal 2:** Run `node test/producer.test.js` (sends messages)
> 3. **Terminal 3:** Run `node test/consumer.test.js` (consumes messages)

**Or test manually with curl:**

```bash
# Produce a message
curl -X POST http://localhost:3000/ingress/my-topic \
  -H "Content-Type: application/json" \
  -d '{"brokerId": "broker_1", "message": {"messageId": "msg-001", "content": "Hello World"}}'

# Consume messages
curl http://localhost:3000/consume/broker_1/my-topic/0

# Commit offset after processing
curl -X POST http://localhost:3000/commit \
  -H "Content-Type: application/json" \
  -d '{"brokerId": "broker_1", "topicId": "my-topic", "partitionId": 0, "consumerId": "test", "offset": 1}'
```

---

## Learn More

- **[Architecture Guide](ARCHITECTURE.md)** — Deep dive into components, data flow & offset semantics

---

## Out of Scope

The following features are **intentionally not implemented** in this project. Panda-Q is designed as a learning project and single-node MVP:

### Distributed Systems
- **Multi-broker clustering** — No distributed coordination between brokers
- **Partition replication** — No replica sets or leader election
- **Cross-datacenter replication** — Single node only
- **Consensus protocols** — No Raft/Paxos for distributed state

### Consumer Features
- **Consumer groups** — No automatic load balancing across consumers
- **Consumer heartbeats** — No detection of dead consumers
- **Auto-rebalancing** — Partitions don't reassign when consumers leave/join
- **Exactly-once semantics** — Only at-least-once is implemented

### Production Features
- **Authentication/Authorization** — No ACLs, SASL, or OAuth
- **TLS/SSL encryption** — Plain HTTP only
- **Rate limiting** — No throttling for producers/consumers
- **Quotas** — No storage or bandwidth limits
- **Schema registry** — No message schema validation

### Operational Features
- **Log compaction** — Logs grow indefinitely
- **Message TTL** — No automatic message expiration
- **Compression** — Messages stored uncompressed
- **Metrics/Monitoring** — No Prometheus endpoint or health checks
- **Admin API** — No runtime topic management

### Performance Optimizations
- **Zero-copy reads** — Standard file I/O only
- **Batch disk writes** — Each message written individually
- **Memory-mapped files** — Using standard fs module
- **Connection pooling** — New connection per request

---

## License

MIT © [Abhijeet Gautam](https://github.com/Abhijeet-Gautam5702)
