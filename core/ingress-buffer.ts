import Queue from "./shared/queue.ts";
import ERROR_CODES from "./shared/error-codes.ts";
import LogFileHandler from "./shared/log-file-handler.ts";
import { ensureFileExists } from "./shared/utils.ts";
import { Message, FilePath, TopicId, LOG_FILE_TYPE } from "./shared/types.ts";
import fs from "fs";
import dotenv from "dotenv"
dotenv.config();

/**
 * Ingress Buffer
 * 
 * The Ingress Buffer is the entry point for all incoming messages from the producer.
 * The job of the ingress buffer is to act as a staging area for all the messages before they are
 * pulled by the broker, so that the HTTP API endpoint resolves quickly.
 * 
 * `logEndOffset`: 
 * The index of the last message that has been inserted into the ingress buffer.
 * This is used to determine the starting point when the ingress buffer is built from the log file.
 * 
 * `readOffset`: 
 * The index of the last message that has been extracted from the ingress buffer.
 * 
 * **NOTE**: `logEndOffset < readOffset` is an invalid state.
 * 
 */
class IngressBuffer {
    public buffer: Queue<Message>;
    private maxLength: number = 100;
    private static readonly logFilePath: FilePath = process.env.INGRESS_LOG_FILE as FilePath;
    private static readonly metadataFilePath: FilePath = process.env.METADATA_FILE as FilePath;
    logEndOffset: number = 0;
    readOffset: number = 0;
    private readonly logHandler: LogFileHandler;

    constructor() {
        const logFileValidation = ensureFileExists(IngressBuffer.logFilePath);
        if (!logFileValidation.isValid) {
            console.error("Failed to initialize log file:", logFileValidation.error);
            process.exit(1);
        }

        const metadataFileValidation = ensureFileExists(IngressBuffer.metadataFilePath);
        if (!metadataFileValidation.isValid) {
            console.error("Failed to initialize metadata file:", metadataFileValidation.error);
            process.exit(1);
        }

        const valuesFromMetadata = this.extractDataFromMetadata();
        if (valuesFromMetadata.isValid) {
            const { logEndOffset, readOffset } = valuesFromMetadata;
            this.logEndOffset = Number(logEndOffset);
            this.readOffset = Number(readOffset);
            if (this.logEndOffset < this.readOffset) {
                console.log("Invalid offset state detected. Log end offset is less than read offset.");
                process.exit(1);
            }
        }

        this.buffer = new Queue<Message>();

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

    // Private methods
    private buildBufferFromLogFile(): boolean | string {
        try {
            console.log("Building ingress buffer from log file...");
            const logFileContent = fs.readFileSync(IngressBuffer.logFilePath, 'utf-8');
            // console.log("Log file content:", logFileContent);
            const logs = logFileContent.split("\n").slice(this.readOffset).filter(log => !!log);
            for (const log of logs) {
                const [brokerId, offset, topicId, messageId, content] = log.split("|");
                this.buffer.enqueue({
                    topicId,
                    messageId,
                    content
                });
            }
            console.log("[DEBUG] Buffer Size After Build In Boot:", this.buffer.size());
            return true;
        } catch (error) {
            console.log("Error building ingress buffer from log file:", error);
            return ERROR_CODES.INGRESS_BUFFER_BUILD_FAILED;
        }
    }

    private extractDataFromMetadata(): { isValid: boolean; logEndOffset?: number; readOffset?: number } {
        try {
            const metadataContent = fs.readFileSync(IngressBuffer.metadataFilePath, 'utf-8');
            const lines = metadataContent.split("\n").filter(line => !!line.trim());

            if (lines.length === 0) {
                fs.appendFileSync(IngressBuffer.metadataFilePath, "ingress|0|0\n");
                return {
                    isValid: true,
                    logEndOffset: 0,
                    readOffset: 0
                };
            }

            const firstLine = lines[0];
            if (!firstLine.startsWith("ingress")) {
                console.log("Malformed metadata file: first line must be the ingress entry.");
                process.exit(1);
            }

            if (firstLine.split("|").length !== 3) {
                console.log("Malformed metadata file: ingress entry must contain exactly three values.");
                process.exit(1);
            }

            const [_, logEndOffset, readOffset] = firstLine.split("|");
            return {
                isValid: true,
                logEndOffset: Number(logEndOffset),
                readOffset: Number(readOffset)
            };
        } catch (error) {
            console.log("Error populating metadata:", error);
            process.exit(1);
        }
    }

    private updateReadOffset(finalOffset?: number): void {
        if (finalOffset) {
            this.readOffset = finalOffset;
        } else {
            this.readOffset += 1;
        }
        fs.writeFileSync(IngressBuffer.metadataFilePath, `ingress|${this.logEndOffset}|${this.readOffset}\n`);
    }

    private updateLogEndOffset(finalOffset?: number): void {
        if (finalOffset) {
            this.logEndOffset = finalOffset;
        } else {
            this.logEndOffset += 1;
        }
        fs.writeFileSync(IngressBuffer.metadataFilePath, `ingress|${this.logEndOffset}|${this.readOffset}\n`);
    }

    // Public methods
    async push(message: Message): Promise<boolean | string> {
        if (this.buffer.size() >= this.maxLength) {
            return ERROR_CODES.INGRESS_BUFFER_FULL;
        }
        const newLogEndOffset = this.logEndOffset + 1;
        const appendResult = await this.logHandler.append(message, newLogEndOffset);
        if (appendResult !== true) {
            return appendResult;
        }
        this.buffer.enqueue(message);
        this.updateLogEndOffset(newLogEndOffset);
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
        this.updateReadOffset(this.readOffset + n);
        return batch;
    }
}

export default IngressBuffer;