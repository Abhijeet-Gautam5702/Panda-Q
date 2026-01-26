import Queue from "./shared/queue.ts";
import ERROR_CODES from "./shared/error-codes.ts";
import LogFileHandler from "./shared/log-file-handler.ts";
import { ensureFileExists } from "./shared/utils.ts";
import { Message, FilePath, TopicId, LOG_FILE_TYPE, Response } from "./shared/types.ts";
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
    private maxLength: number = 200_000_000;
    private static readonly logFilePath: FilePath = process.env.INGRESS_LOG_FILE as FilePath;
    private static readonly metadataFilePath: FilePath = process.env.INGRESS_METADATA_FILE as FilePath;
    logEndOffset: number = 0;
    readOffset: number = 0;
    private readonly logHandler: LogFileHandler;

    constructor() {
        console.log("[IngressBuffer] Initializing IngressBuffer...");
        const logFileValidation = ensureFileExists(IngressBuffer.logFilePath);
        if (!logFileValidation.isValid) {
            console.error("Failed to initialize log file:", logFileValidation.error);
            process.exit(1);
        }
        console.log(`[IngressBuffer] Ingress log file validated: ${IngressBuffer.logFilePath}`);

        const metadataFileValidation = ensureFileExists(IngressBuffer.metadataFilePath);
        if (!metadataFileValidation.isValid) {
            console.error("Failed to initialize metadata file:", metadataFileValidation.error);
            process.exit(1);
        }
        console.log(`[IngressBuffer] Ingress metadata file validated: ${IngressBuffer.metadataFilePath}`);


        const valuesFromMetadata = this.extractDataFromMetadata();
        if (!valuesFromMetadata.success) {
            console.error("Failed to extract metadata:", valuesFromMetadata.errorCode, valuesFromMetadata.error);
            process.exit(1);
        }

        const { logEndOffset, readOffset } = valuesFromMetadata.data;
        this.logEndOffset = logEndOffset;
        this.readOffset = readOffset;
        console.log(`[IngressBuffer] Ingress metadata loaded - logEndOffset: ${logEndOffset}, readOffset: ${readOffset}`);
        if (this.logEndOffset < this.readOffset) {
            console.error("Invalid offset state detected. Log end offset is less than read offset.");
            process.exit(1);
        }

        this.buffer = new Queue<Message>();

        this.logHandler = new LogFileHandler({
            label: LOG_FILE_TYPE.INGRESS_BUFFER,
            filePath: IngressBuffer.logFilePath
        });

        // Build the ingress buffer from the offset and the log file
        const buildResult = this.buildBufferFromLogFile();
        if (!buildResult.success) {
            console.error("Error building ingress buffer from log file:", buildResult.errorCode, buildResult.error);
            process.exit(1);
        }
        console.log(`[IngressBuffer] IngressBuffer initialized successfully with ${this.buffer.size()} message(s)`);
    }

    // Private methods
    private buildBufferFromLogFile(): Response<boolean> {
        try {
            console.log("Building ingress buffer from log file...");
            const logFileContent = fs.readFileSync(IngressBuffer.logFilePath, 'utf-8');
            const logs = logFileContent.split("\n").slice(this.readOffset).filter(log => !!log);

            for (const log of logs) {
                const [brokerId, offset, topicId, messageId, content] = log.split("|");
                this.buffer.enqueue({
                    topicId,
                    messageId: Number(messageId),
                    content
                });
            }

            return {
                success: true,
                data: true
            };
        } catch (error) {
            return {
                success: false,
                errorCode: ERROR_CODES.BUFFER_BUILD_FAILED,
                error: error
            };
        }
    }

    private extractDataFromMetadata(): Response<{ logEndOffset: number; readOffset: number }> {
        try {
            const metadataContent = fs.readFileSync(IngressBuffer.metadataFilePath, 'utf-8');
            const lines = metadataContent.split("\n").filter(line => !!line.trim());

            if (lines.length === 0) {
                fs.appendFileSync(IngressBuffer.metadataFilePath, "ingress|0|0\n");
                return {
                    success: true,
                    data: {
                        logEndOffset: 0,
                        readOffset: 0
                    }
                };
            }

            const firstLine = lines[0];
            if (!firstLine.startsWith("ingress")) {
                return {
                    success: false,
                    errorCode: ERROR_CODES.INVALID_FILE_PATH,
                    error: new Error("Malformed metadata file: first line must be the ingress entry")
                };
            }

            if (firstLine.split("|").length !== 3) {
                return {
                    success: false,
                    errorCode: ERROR_CODES.INVALID_FILE_PATH,
                    error: new Error("Malformed metadata file: ingress entry must contain exactly three values")
                };
            }

            const [_, logEndOffset, readOffset] = firstLine.split("|");
            return {
                success: true,
                data: {
                    logEndOffset: Number(logEndOffset),
                    readOffset: Number(readOffset)
                }
            };
        } catch (error) {
            return {
                success: false,
                errorCode: ERROR_CODES.FILE_NOT_FOUND,
                error: error
            };
        }
    }

    private updateReadOffset(finalOffset?: number): Response<boolean> {
        try {
            if (finalOffset) {
                this.readOffset = finalOffset;
            } else {
                this.readOffset += 1;
            }
            fs.writeFileSync(IngressBuffer.metadataFilePath, `ingress|${this.logEndOffset}|${this.readOffset}\n`);
            return {
                success: true,
                data: true
            };
        } catch (error) {
            return {
                success: false,
                errorCode: ERROR_CODES.LOG_FILE_APPEND_FAILED,
                error: error
            };
        }
    }

    private updateLogEndOffset(finalOffset?: number): Response<boolean> {
        try {
            if (finalOffset) {
                this.logEndOffset = finalOffset;
            } else {
                this.logEndOffset += 1;
            }
            fs.writeFileSync(IngressBuffer.metadataFilePath, `ingress|${this.logEndOffset}|${this.readOffset}\n`);
            return {
                success: true,
                data: true
            };
        } catch (error) {
            return {
                success: false,
                errorCode: ERROR_CODES.LOG_FILE_APPEND_FAILED,
                error: error
            };
        }
    }

    // Public methods
    async push(message: Message): Promise<Response<boolean>> {
        try {
            if (this.buffer.size() >= this.maxLength) {
                console.log(`[IngressBuffer] Ingress buffer full (${this.buffer.size()}/${this.maxLength})`);
                return {
                    success: false,
                    errorCode: ERROR_CODES.INGRESS_BUFFER_FULL,
                    error: new Error("Ingress buffer has reached maximum capacity")
                };
            }

            const newLogEndOffset = this.logEndOffset + 1;
            const appendResult = await this.logHandler.append(message, newLogEndOffset);

            if (!appendResult.success) {
                return appendResult;
            }

            this.buffer.enqueue(message);

            const updateResult = this.updateLogEndOffset(newLogEndOffset);
            if (!updateResult.success) {
                return updateResult;
            }

            return {
                success: true,
                data: true
            };
        } catch (error) {
            return {
                success: false,
                errorCode: ERROR_CODES.BUFFER_BUILD_FAILED,
                error: error
            };
        }
    }

    batchExtract(batchSize: number): Response<Message[]> {
        try {
            if (this.buffer.isEmpty()) {
                // Buffer is empty, this is normal during idle periods
                return {
                    success: false,
                    errorCode: ERROR_CODES.INGRESS_BUFFER_EMPTY,
                    error: new Error("Ingress buffer is empty")
                };
            }

            const batch: Message[] = [];
            const n = Math.min(batchSize, this.buffer.size());

            for (let i = 0; i < n; i++) {
                const message = this.buffer.dequeue();
                if (message) {
                    batch.push(message);
                }
            }

            const updateResult = this.updateReadOffset(this.readOffset + n);
            if (!updateResult.success) {
                return updateResult;
            }

            console.log(`[IngressBuffer] Extracted ${n} message(s) from ingress buffer (remaining: ${this.buffer.size()})`);
            return {
                success: true,
                data: batch
            };
        } catch (error) {
            return {
                success: false,
                errorCode: ERROR_CODES.BUFFER_BUILD_FAILED,
                error: error
            };
        }
    }
}

export default IngressBuffer;