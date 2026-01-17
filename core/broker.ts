import IngressBuffer from "./ingress-buffer";

// Types
export type PartitionId = string;
export type TopicId = string;

// Broker Class
class Broker {

}

// Helper Functions In Broker Class
function partitioner(message: string): PartitionId {
    return "1";
}

function topicSorter(message:string): TopicId {
    return "1";
}

export default Broker;