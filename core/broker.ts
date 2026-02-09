import IngressBuffer from "./ingress-buffer.js";
import ERROR_CODES from "./shared/error-codes.js";
import { TopicId, BrokerId, Response, ConsumerId, PartitionId } from "./shared/types.js";
import Topic from "./topic.js";
import { internalTPCMap } from "./main.js";
import { writeTPCLog } from "./shared/tpc-helper.js";

/**
 * Broker Class
 * 
 * The job of the broker is to extract/receive messages from the ingress buffer and
 * sort them based on the topics.
 */
class Broker {
    private readonly brokerId: BrokerId;
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
        console.log(`[Broker] Setting up topics from TPC Map`);

        // Use internalTPCMap instead of config file
        for (const [topicId, partitionMap] of internalTPCMap) {
            const noOfPartitions = partitionMap.size;
            console.log(`[Broker] Creating topic: ${topicId} with ${noOfPartitions} partition(s)`);
            this.topics.set(topicId, new Topic(topicId, noOfPartitions));
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

    async registerConsumer(topicId: TopicId, consumerId: ConsumerId): Promise<Response<{ partitionId: PartitionId }>> {
        try {
            const partitionMap = internalTPCMap.get(topicId);
            if (!partitionMap) {
                return {
                    success: false,
                    errorCode: ERROR_CODES.TOPIC_NOT_FOUND,
                    error: new Error(`Topic ${topicId} not found in TPC Map`)
                };
            }

            // Find an unassigned partition or the partition with this consumer
            let assignedPartitionId: PartitionId | null = null;

            // First, check if consumer is already assigned to a partition
            for (const [partitionId, existingConsumerId] of partitionMap) {
                if (existingConsumerId === consumerId) {
                    console.log("Consumer Already Registered With Partition:", partitionId)
                    assignedPartitionId = partitionId;
                    break;
                }
            }

            // If not already assigned, find an empty partition
            if (assignedPartitionId === null) {
                for (const [partitionId, existingConsumerId] of partitionMap) {
                    if (existingConsumerId === "") {
                        assignedPartitionId = partitionId;
                        partitionMap.set(partitionId, consumerId);

                        // Persist TPC Map to TPC.log
                        writeTPCLog(internalTPCMap);

                        console.log(`[Broker] Consumer ${consumerId} assigned to partition ${partitionId} for topic ${topicId}`);
                        break;
                    }
                }
            }

            if (assignedPartitionId === null) {
                return {
                    success: false,
                    errorCode: ERROR_CODES.NO_PARTITION_AVAILABLE,
                    error: new Error(`No available partition for topic ${topicId}`)
                };
            }

            return {
                success: true,
                data: { partitionId: assignedPartitionId }
            };
        } catch (error) {
            return {
                success: false,
                errorCode: ERROR_CODES.UNKNOWN_ERROR,
                error: error
            };
        }
    }

    getTopic(topicId: TopicId): Topic | undefined {
        return this.topics.get(topicId);
    }

    getStats(): { brokerId: BrokerId; topicCount: number; topics: any[]; ingressBuffer: any } {
        const topicStats: any[] = [];
        for (const [topicId, topic] of this.topics) {
            topicStats.push(topic.getStats());
        }
        return {
            brokerId: this.brokerId,
            topicCount: this.topics.size,
            topics: topicStats,
            ingressBuffer: {
                bufferSize: this.ingressBuffer.getBufferSize(),
                logEndOffset: this.ingressBuffer.getLogEndOffset(),
                readOffset: this.ingressBuffer.getReadOffset()
            }
        };
    }
}

export default Broker;