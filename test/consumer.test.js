import Consumer from '../dist/client/consumer.js';
import { MetricsCollector } from './metrics-collector.js';

const collector = new MetricsCollector();

async function main() {
    const pandaConsumer = new Consumer(
        "http://localhost:3000",
        "broker_1",
        "latency-test-consumer", // Fixed ID to avoid exhausting partitions
        "latency-test"
    );

    console.log("Consumer started. Waiting for registration...");
    await new Promise(resolve => setTimeout(resolve, 3000)); // Wait for registration
    console.log("Registration complete. Starting consumption...");

    // // Auto-exit after 10 seconds
    // setTimeout(() => {
    //     console.log('\n\nAuto-stopping consumer after 30 seconds...');
    //     const stats = collector.saveToFile();
    //     console.log('\nFinal Stats:', stats);
    //     process.exit();
    // }, 30000);

    // Continuous consumption loop
    setInterval(async () => {
        const response = await pandaConsumer.batchConsume();

        const responseData = response.data?.data || response.data;
        if (response.success && responseData && responseData.messages) {
            const messages = Array.isArray(responseData.messages)
                ? responseData.messages
                : [responseData.messages];

            if (messages.length === 0 && collector.latencies.length >= 49500) {
                console.log("No message detected to be consumed. Stopping.");
                const stats = collector.saveToFile();
                console.log('\nFinal Stats:', stats);
                process.exit();
            }

            if (messages.length > 0) {
                const now = Date.now();

                messages.forEach(msg => {
                    let content = msg.content;
                    if (typeof content === 'string') {
                        try { content = JSON.parse(content); } catch (e) { }
                    }
                    if (content && content.createdAt) {
                        const latency = now - content.createdAt;
                        collector.recordLatency(latency);
                    }
                });

                process.stdout.write(`\rReceived: ${messages.length} messages. Total Processed: ${collector.latencies.length}\n`);

                // Commit offset
                await pandaConsumer.commitOffset(responseData.endOffset);
            }
        }
    }, 200);

    // Handle exit
    process.on('SIGINT', () => {
        console.log('\n\nStopping consumer...');
        const stats = collector.saveToFile();
        console.log('\nFinal Stats:', stats);
        process.exit();
    });
}

main();