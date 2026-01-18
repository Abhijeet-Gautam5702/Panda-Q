import Queue from "./shared/queue.ts";
import { TopicId } from "./broker.ts";
import ERROR_CODES from "./shared/error-codes.ts";
import LogFileHandler, { LOG_FILE_TYPE } from "./shared/log-file-handler.ts";
import fs from "fs";
import dotenv from "dotenv"
dotenv.config();

// Types
export type Message = {
    topicId: TopicId;
    messageId: string;
    content: string;
}

export type FilePath = string;

/**
 * Ingress Buffer
 * 
 * The Ingress Buffer is the entry point for all incoming messages from the producer.
 * The job of the ingress buffer is to act as a staging area for all the messages before they are
 * pulled by the broker, so that the HTTP API endpoint resolves quickly.
 * 
 * Offset: The index of the last message that has been processed in the ingress buffer.
 * This is used to determine the starting point when the ingress buffer is built from the log file.
 * 
 */
class IngressBuffer {
    public buffer: Queue<Message>;
    private maxLength: number = 10;
    private static readonly logFilePath: FilePath = process.env.INGRESS_LOG_FILE as FilePath;
    private static readonly metadataFilePath: FilePath = process.env.INGRESS_METADATA_FILE as FilePath;
    offset: number;
    private readonly logHandler: LogFileHandler;

    constructor() {
        this.buffer = new Queue<Message>();
        this.offset = this.getOffset();
        this.logHandler = new LogFileHandler({
            label: LOG_FILE_TYPE.INGRESS_BUFFER,
            filePath: IngressBuffer.logFilePath
        });
        // Build the ingress buffer from the offset and the log file
        const buildResult = this.buildBufferFromLogFile();
        if (buildResult !== true) {
            console.log("Error building ingress buffer from log file:", buildResult);
            process.exit(1);
        }
    }

    private buildBufferFromLogFile(): boolean | string {
        try {
            const logFileContent = fs.readFileSync(IngressBuffer.logFilePath, 'utf-8');
            // console.log("Log file content:", logFileContent);
            const logs = logFileContent.split("\n").slice(this.offset).filter(log => !!log);
            for (const log of logs) {
                const [brokerId, offset, topicId, messageId, content] = log.split("|");
                this.buffer.enqueue({
                    topicId,
                    messageId,
                    content
                });
            }
            return true;
        } catch (error) {
            console.log("Error building ingress buffer from log file:", error);
            return ERROR_CODES.INGRESS_BUFFER_BUILD_FAILED;
        }
    }

    private getOffset(): number {
        if (fs.existsSync(IngressBuffer.metadataFilePath)) {
            const metadata = JSON.parse(fs.readFileSync(IngressBuffer.metadataFilePath, 'utf-8'));
            if (metadata?.offset) {
                return metadata.offset;
            }
        }
        return 0;
    }

    private updateOffset(finalOffset: number): void {
        if (finalOffset) {
            this.offset = finalOffset;
        } else {
            this.offset += 1;
        }
        fs.writeFileSync(IngressBuffer.metadataFilePath, JSON.stringify({ offset: this.offset }));
    }

    async push(message: Message): Promise<boolean | string> {
        if (this.buffer.size() >= this.maxLength) {
            return ERROR_CODES.INGRESS_BUFFER_FULL;
        }
        const appendResult = await this.logHandler.append(message, this.offset + this.buffer.size() + 1);
        if (appendResult !== true) {
            return appendResult;
        }
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
        this.updateOffset(this.offset + n);
        return batch;
    }
}

export default IngressBuffer;