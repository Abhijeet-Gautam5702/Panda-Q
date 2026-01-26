import { createHash } from "node:crypto";
import Partition from "./partition.ts";
import MockConsumer from "./consumer.ts";
import ERROR_CODES from "./shared/error-codes.ts";
import { Message, PartitionId, Response, TopicId } from "./shared/types.ts";

class Topic {
    private readonly topicId: TopicId;
    private readonly topicName: string;
    private readonly noOfPartitions: number;
    private partitions: Map<PartitionId, Partition>;
    // private consumers: MockConsumer[];

    constructor(topicId: TopicId, topicName: string, noOfPartitions: number) {
        console.log(`[Topic] Initializing Topic: ${topicName} (ID: ${topicId})`);
        this.topicId = topicId;
        this.topicName = topicName;
        this.noOfPartitions = noOfPartitions;
        this.partitions = new Map<PartitionId, Partition>();
        // this.consumers = [];
        this.setupPartitions();
        console.log(`[Topic] Topic ${topicName} initialized with ${noOfPartitions} partition(s)`);
    }

    private async setupPartitions(): Promise<void> {
        // Create partitions
        for (let i = 0; i < this.noOfPartitions; i++) {
            this.partitions.set(i, new Partition(i, this.topicId));
        }

        // Create and start one consumer per partition
        // console.log(`[Topic] Starting ${this.noOfPartitions} consumer(s) for topic ${this.topicName}`);
        // for (let i = 0; i < this.noOfPartitions; i++) {
        //     const partition = this.partitions.get(i);
        //     if (partition) {
        //         const consumerId = `consumer_${this.topicId}_p${i}`;
        //         const consumer = new MockConsumer(consumerId, partition, this.topicId, i);
        //         this.consumers.push(consumer);
        //         consumer.start(); // Start consumer in background
        //     }
        // }
    }

    private assignPartition(messageId: number): Response<PartitionId> {
        try {
            // Generate a secure hash of the messageId
            const hash = createHash('sha256')
                .update(messageId.toString())
                .digest('hex');

            // Convert hash to a number for partition assignment
            const hashValue = parseInt(hash.substring(0, 8), 16);
            const partitionId = hashValue % this.noOfPartitions;

            const partitionExists = this.partitions.has(partitionId);
            if (!partitionExists) {
                return {
                    success: false,
                    errorCode: ERROR_CODES.PARTITION_NOT_FOUND,
                    error: `Partition ${partitionId} not found`
                }
            }

            return {
                success: true,
                data: partitionId
            }
        } catch (error) {
            return {
                success: false,
                errorCode: ERROR_CODES.UNKNOWN_ERROR,
                error: error
            }
        }
    }

    async push(message: Message): Promise<Response<void>> {
        const partitionIdResponse = this.assignPartition(message.messageId);
        if (!partitionIdResponse.success) {
            console.log(`[Topic] Failed to assign partition for message ${message.messageId}: ${partitionIdResponse.errorCode}`);
            return partitionIdResponse;
        }
        const partitionId = partitionIdResponse.data;
        const partition = this.partitions.get(partitionId);
        if (!partition) {
            console.log(`[Topic] Partition ${partitionId} not found in topic ${this.topicId}`);
            return {
                success: false,
                errorCode: ERROR_CODES.PARTITION_NOT_FOUND,
                error: `Partition ${partitionId} not found`
            }
        }
        console.log(`[Topic] Pushing message ${message.messageId} to topic ${this.topicId}, partition ${partitionId}`);
        return await partition.push(message);
    }
}

export default Topic;