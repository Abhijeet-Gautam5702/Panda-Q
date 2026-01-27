import IngressBuffer from "./ingress-buffer.ts";
import ERROR_CODES from "./shared/error-codes.ts";
import { TopicId, BrokerId, FilePath, Response } from "./shared/types.ts";
import Topic from "./topic.ts";
import fs from "fs";

/**
 * Broker Class
 * 
 * The job of the broker is to extract/receive messages from the ingress buffer and
 * sort them based on the topics.
 */
class Broker {
    private readonly brokerId: BrokerId;
    private static readonly configLogPath: FilePath = process.env.CONFIG_LOG_FILE as FilePath;
    private topics: Map<TopicId, Topic>;
    readonly ingressBuffer: IngressBuffer;

    constructor(brokerId: BrokerId) {
        console.log(`[Broker] Initializing Broker: ${brokerId}`);
        this.brokerId = brokerId;
        this.ingressBuffer = new IngressBuffer();
        this.topics = new Map<TopicId, Topic>();
        this.setupTopics();
        console.log(`[Broker] Broker ${brokerId} initialized successfully with ${this.topics.size} topics`);
    }

    // Private methods
    private setupTopics(): void {
        console.log(`[Broker] Setting up topics from config: ${Broker.configLogPath}`);
        const configLogContent = fs.readFileSync(Broker.configLogPath, 'utf-8');
        const lines = configLogContent.split("\n").filter(line => {
            const [label] = line?.trim()?.split("|", 1);
            return label === "topic_config";
        });

        console.log(`[Broker] Found ${lines.length} topic configuration(s)`);
        for (const line of lines) {
            const [_, topicId, topicName, noOfPartitions] = line?.trim()?.split("|", 4) || [];
            console.log(`[Broker] Creating topic: ${topicName} (ID: ${topicId}) with ${noOfPartitions} partition(s)`);
            this.topics.set(topicId, new Topic(topicId, topicName, Number(noOfPartitions)));
        }
    }

    // Public methods
    async start(): Promise<Response<void>> {
        console.log(`[Broker] Broker ${this.brokerId} started. Entering main processing loop...`);
        let cycleCount = 0;
        while (true) {
            cycleCount++;
            // fetch a batch of messages from the ingress buffer
            const batchResponse = this.ingressBuffer.batchExtract(100);
            if (!batchResponse.success) {
                // Buffer is empty, just continue to next cycle
                await new Promise((resolve) => setTimeout(resolve, 100));
                continue;
            }

            if (batchResponse.data.length > 0) {
                console.log(`[BROKER] Cycle ${cycleCount}: Extracted ${batchResponse.data.length} message(s) from ingress buffer`);
            }

            // segregate messages according to topics
            for (const message of batchResponse.data) {
                const topicIdOfMessage = message.topicId;
                const topic = this.topics.get(topicIdOfMessage);
                if (!topic) {
                    console.log(`[Broker] Topic ${topicIdOfMessage} not found for message ${message.messageId}`);
                    continue;
                }
                await topic.push(message);
            }

            // yield back to the event loop (avoid thread blocking)
            await new Promise((resolve) => setTimeout(resolve, 100));
        }
    }
}

export default Broker;