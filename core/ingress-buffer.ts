import Queue from "./shared/queue";
import { TopicId } from "./broker";
import ERROR_CODES from "./shared/error-codes";

// Types
export type Message = {
    topicId: TopicId;
    message: string;
}

export type FilePath = string;

/**
 * Ingress Buffer
 * 
 * The Ingress Buffer is the entry point for all incoming messages from the producer.
 * The job of the ingress buffer is to act as a staging area for all the messages before they are
 * pulled by the broker, so that the HTTP API endpoint resolves quickly.
 * 
 */
class IngressBuffer {
    public buffer: Queue<Message>;
    private maxLength: number = 2000;
    private ingressLogFile: FilePath;
    private offset: number;

    constructor(offset: number, ingressLogFile: FilePath) {
        this.buffer = new Queue<Message>();
        this.offset = offset;
        this.ingressLogFile = ingressLogFile;
    }
    
    push(message: Message): boolean | string {
        if (this.buffer.size() >= this.maxLength) {
            return ERROR_CODES.INGRESS_BUFFER_FULL;
        }
        // TODO: Write to the ingress-log-file for persistence
        this.buffer.enqueue(message);
        return true;
    }

    batchExtract(batchSize: number): Message[] | string {
        if (this.buffer.isEmpty()) {
            return ERROR_CODES.INGRESS_BUFFER_EMPTY;
        }
        const batch: Message[] = [];
        const n = Math.min(batchSize, this.buffer.size());
        for (let i = 0; i < n; i++) {
            const message = this.buffer.dequeue();
            if (message) {
                batch.push(message);
            }
        }
        return batch;
    }
}

export default IngressBuffer;