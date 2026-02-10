import Producer from '../dist/client/producer.js';

const pandaProducer = new Producer(
    "broker_1",
    "http://localhost:3000",
    "latency-test"
)


const args = process.argv.slice(2);
const RATE = parseInt(args.find(arg => arg.startsWith('--rate='))?.split('=')[1] || '1000');
const DURATION_SECONDS = parseInt(args.find(arg => arg.startsWith('--duration='))?.split('=')[1] || '10');

console.log(`Starting Load Test: ${RATE} msg/sec for ${DURATION_SECONDS} seconds`);

async function main() {
    const names = Array.from({ length: 10 }, () => Math.random().toString(36).substring(2, 9));
    let totalSent = 0;
    const startTime = Date.now();

    // Calculate interval to send batch to achieve target rate
    // We'll send batches every 100ms to be efficient
    const BATCH_INTERVAL_MS = 100;
    const BATCH_SIZE = Math.ceil(RATE * (BATCH_INTERVAL_MS / 1000));

    const interval = setInterval(async () => {
        const elapsed = (Date.now() - startTime) / 1000;
        if (elapsed >= DURATION_SECONDS) {
            clearInterval(interval);
            console.log(`Test Completed. Total Sent: ${totalSent}`);
            return;
        }

        // Send batch
        const promises = [];
        for (let i = 0; i < BATCH_SIZE; i++) {
            const idx = (Math.ceil(Math.random() * 10) % names.length);
            const message = {
                messageId: `msg-${Date.now()}-${i}-${Math.random().toString(36).substring(7)}`,
                content: {
                    createdAt: Date.now(), // Critical for latency calculation
                    body: {
                        name: names[idx],
                        age: Math.random() * 100
                    }
                }
            };
            promises.push(pandaProducer.produce(message));
        }

        await Promise.all(promises);
        totalSent += BATCH_SIZE;
        process.stdout.write(`\rSent: ${totalSent} messages...`);
    }, BATCH_INTERVAL_MS);
}

main();
