import { createHash } from "node:crypto";
import Partition from "./partition.ts";
import ERROR_CODES from "./shared/error-codes.ts";
import { Message, PartitionId, Response, TopicId } from "./shared/types.ts";

class Topic {
    private readonly topicId: TopicId;
    private readonly topicName: string;
    private readonly noOfPartitions: number;
    private partitions: Map<PartitionId, Partition>;

    constructor(topicId: TopicId, topicName: string, noOfPartitions: number) {
        this.topicId = topicId;
        this.topicName = topicName;
        this.noOfPartitions = noOfPartitions;
        this.partitions = new Map<PartitionId, Partition>();
        this.setupPartitions();
    }

    private async setupPartitions(): Promise<void> {
        for (let i = 0; i < this.noOfPartitions; i++) {
            this.partitions.set(i, new Partition(i));
        }
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

    push(message: Message): Response<void> {
        const partitionIdResponse = this.assignPartition(message.messageId);
        if (!partitionIdResponse.success) {
            return partitionIdResponse;
        }
        const partitionId = partitionIdResponse.data;
        const partition = this.partitions.get(partitionId);
        if (!partition) {
            return {
                success: false,
                errorCode: ERROR_CODES.PARTITION_NOT_FOUND,
                error: `Partition ${partitionId} not found`
            }
        }
        partition.push(message);
        return {
            success: true,
            data: undefined
        };
    }
}

export default Topic;