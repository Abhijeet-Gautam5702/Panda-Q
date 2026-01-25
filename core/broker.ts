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
    private readonly configLogPath: FilePath;
    private readonly ingressBuffer: IngressBuffer;
    private topics: Map<TopicId, Topic>;

    constructor(brokerId: BrokerId) {
        this.brokerId = brokerId;
        this.configLogPath = process.env.CONFIG_LOG_FILE as FilePath;
        this.ingressBuffer = new IngressBuffer();
        this.topics = new Map<TopicId, Topic>();
        this.setupTopics();
    }

    // Private methods
    private setupTopics(): void {
        const configLogContent = fs.readFileSync(this.configLogPath, 'utf-8');
        const lines = configLogContent.split("\n").filter(line => {
            const [label] = line?.trim()?.split("|", 1);
            return label === "topic_label";
        });

        for (const line of lines) {
            const [_, topicId, topicName, noOfPartitions] = line?.trim()?.split("|", 4) || [];
            this.topics.set(topicId, new Topic(topicId, topicName, Number(noOfPartitions)));
        }
    }

    // Public methods
    async start(): Promise<Response<void>> {
        while (true) {
            // fetch a batch of messages from the ingress buffer
            const batchResponse = this.ingressBuffer.batchExtract(100);
            if (!batchResponse.success) {
                return {
                    success: false,
                    errorCode: batchResponse.errorCode,
                    error: batchResponse.error
                }
            }

            // yield back to the event loop (avoid thread blocking)
            await new Promise((resolve) => setTimeout(resolve, 100));

            // segregate messages according to topics
            for (const message of batchResponse.data) {
                const topicIdOfMessage = message.topicId;
                const topic = this.topics.get(topicIdOfMessage);
                if (!topic) {
                    return {
                        success: false,
                        errorCode: ERROR_CODES.TOPIC_NOT_FOUND,
                        error: `Topic ${topicIdOfMessage} not found`
                    }
                }
                topic.push(message);
            }
        }
    }
}

export default Broker;