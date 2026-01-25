import { Message, PartitionId, Response } from "./shared/types.ts";

class Partition {
    private readonly partitionId: PartitionId;

    constructor(partitionId: PartitionId) {
        this.partitionId = partitionId;
    }

    push(message: Message): Response<void> {
        return {
            success: true,
            data: undefined
        };
    }
}

export default Partition;
