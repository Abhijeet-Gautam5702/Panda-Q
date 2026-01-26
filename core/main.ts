import dotenv from "dotenv";
import Broker from "./broker.ts";
import { Bootstrap } from "./bootstrap.ts";
dotenv.config();

/**
 * Mock Producer - Simulates message production for testing
 * Runs every 5 seconds and inserts 5 messages into the ingress buffer
 */
function startMockProducer(broker: Broker, topicIds: string[]): void {
    console.log("\n[Main] Starting mock producer (100 messages every 0.1 seconds)...");

    let messageCounter = 1;

    setInterval(async () => {
        console.log(`\n[PRODUCER] Producing 100 messages...`);

        for (let i = 0; i < 100; i++) {
            // Randomly select a topic
            const topicId = topicIds[Math.floor(Math.random() * topicIds.length)];

            const message = {
                topicId,
                messageId: messageCounter++,
                content: `Message ${messageCounter - 1} for topic ${topicId} - ${new Date().toISOString()}`
            };

            const result = await broker.getIngressBuffer().push(message);

            if (result.success) {
                console.log(`[PRODUCER] ✓ Pushed message ${message.messageId} to topic ${topicId}`);
            } else {
                console.error(`[PRODUCER] ✗ Failed to push message: ${result.errorCode}`);
            }
        }

        console.log(`[PRODUCER] Batch complete. Total messages produced: ${messageCounter - 1}\n`);
    }, 100000);
}


async function main() {
    try {
        console.log("Server Start Time: ", process.hrtime.bigint());
        console.log("====== BOOTSTRAPPING PANDA-Q SERVER ======");

        // Parse configuration and initialize/validate data directory
        const config = await Bootstrap.bootstrap();

        console.log(`[Main] Configuration validated successfully`);
        console.log(`[Main] Broker ID: ${config.brokerId}`);
        console.log(`[Main] Topics configured: ${config.topics.length}`);

        // Start the broker instance
        const broker = new Broker(config.brokerId);

        // Start mock producer to simulate message production
        startMockProducer(broker, config.topics.map(t => t.id));

        await broker.start();

    } catch (error) {
        console.error("\n❌ Bootstrap failed:", error);
        console.error("\nServer cannot start due to configuration/data directory issues.");
        process.exit(1);
    }
}

process.on("SIGINT", () => {
    console.log("\n====== SHUTTING DOWN PANDA-Q SERVER ======");
    console.log("Server End Time: ", process.hrtime.bigint());
    console.log("Memory Usage: ", process.memoryUsage());
    process.exit(0);
});

main().catch(console.error);