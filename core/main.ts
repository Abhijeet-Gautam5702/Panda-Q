import dotenv from "dotenv";
import Broker from "./broker.js";
import { Bootstrap } from "./bootstrap.js";
import Server from "./server.js";
import { ConsumerId, PartitionId, TopicId } from "./shared/types.js";
import getEnv from "./shared/env-config.js";
dotenv.config();

/**
 * Internal Topic-Partition-Consumer Map
 * 
 * This map is used to maintain the mapping of topics to partitions and consumers.
 * It is used to determine which consumer should consume a message from a partition.
 */
export const internalTPCMap = new Map<TopicId, Map<PartitionId, ConsumerId>>();

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

        // Start HTTP server to accept producer/consumer connections
        const port = getEnv().PORT ? parseInt(getEnv().PORT) : 3000;
        const server = new Server(broker, port);
        server.start();

        console.log(`[Main] HTTP Server started on port ${port}`);
        // console.log(`[Main] Starting broker processing loop...`);
        await broker.start();

    } catch (error) {
        console.error("\nBootstrap failed:", error);
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