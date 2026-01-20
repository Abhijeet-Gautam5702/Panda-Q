import { PartitionId, TopicId, BrokerId } from "./shared/types.ts";

// Broker Class
class Broker {

}

// Helper Functions In Broker Class
function partitioner(message: string): PartitionId {
    return "1";
}

function topicSorter(message: string): TopicId {
    return "1";
}

export default Broker;