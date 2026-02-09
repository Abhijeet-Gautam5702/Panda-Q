import { createHash } from "node:crypto";
import Partition from "./partition.js";
import ERROR_CODES from "./shared/error-codes.js";
import { Message, PartitionId, Response, TopicId } from "./shared/types.js";

class Topic {
    private readonly topicId: TopicId;
    private readonly noOfPartitions: number;
    private partitions: Map<PartitionId, Partition>;

    constructor(topicId: TopicId, noOfPartitions: number) {
        console.log(`[Topic] Initializing Topic: ${topicId}`);
        this.topicId = topicId;
        this.noOfPartitions = noOfPartitions;
        this.partitions = new Map<PartitionId, Partition>();
        this.setupPartitions();
        console.log(`[Topic] Topic ${topicId} initialized with ${noOfPartitions} partition(s)`);
    }

    private setupPartitions(): void {
        // Create partitions
        for (let i = 0; i < this.noOfPartitions; i++) {
            this.partitions.set(i, new Partition(i, this.topicId));
        }
    }

    private assignPartition(messageId: string): Response<PartitionId> {
        try {
            // Generate a secure hash of the string messageId
            const hash = createHash('sha256')
                .update(messageId)
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

    getPartition(partitionId: PartitionId): Partition | undefined {
        return this.partitions.get(partitionId);
    }

    getStats(): { topicId: TopicId; partitionCount: number; partitions: any[] } {
        const partitionStats: any[] = [];
        for (const [partitionId, partition] of this.partitions) {
            partitionStats.push({
                partitionId,
                ...partition.getStats()
            });
        }
        return {
            topicId: this.topicId,
            partitionCount: this.noOfPartitions,
            partitions: partitionStats
        };
    }
}

export default Topic;