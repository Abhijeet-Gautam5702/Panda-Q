import fs from "fs";
import LogFileHandler from "./shared/log-file-handler.js";
import Queue from "./shared/queue.js";
import ERROR_CODES from "./shared/error-codes.js";
import { FilePath, LOG_FILE_TYPE, Message, PartitionId, Response } from "./shared/types.js";
import { ensureFileExists } from "./shared/utils.js";

/**
 * Partition
 * 
 * Each partition maintains its own in-memory buffer and a log file for crash recovery.
 * Messages are distributed across partitions based on a hash of their messageId.
 * 
 * `logEndOffset`: 
 * The index of the last message that has been inserted into the partition buffer.
 * This is used to determine the starting point when the partition buffer is built from the log file.
 * 
 * `readOffset`: 
 * The index of the last message that has been extracted from the partition buffer.
 * 
 * **NOTE**: `logEndOffset < readOffset` is an invalid state.
 * 
 */
class Partition {
    private readonly partitionId: PartitionId;
    private readonly topicId: string;
    private logEndOffset: number;
    private readOffset: number;
    private readonly buffer: Queue<Message>;
    private readonly maxBufferSize: number = 100_000_000;
    private readonly logFilePath: FilePath;
    private readonly metadataFilePath: FilePath;
    private readonly logHandler: LogFileHandler;


    constructor(partitionId: PartitionId, topicId: string) {
        console.log(`[Partition] Initializing Partition ${partitionId} for topic ${topicId}`);
        this.partitionId = partitionId;
        this.topicId = topicId;

        // Build dynamic file path for this partition
        const dataStorageVolume = process.env.DATA_STORAGE_VOLUME as FilePath;
        this.logFilePath = `${dataStorageVolume}/topics/topic_${topicId}/partition_${partitionId}.log` as FilePath;
        console.log(`[Partition] Partition log file path: ${this.logFilePath}`);

        const logFileValidation = ensureFileExists(this.logFilePath);
        if (!logFileValidation.isValid) {
            console.error("Failed to initialize log file:", logFileValidation.error);
            process.exit(1);
        }

        // Build dynamic metadata file path for this topic
        this.metadataFilePath = `${dataStorageVolume}/topics/topic_${topicId}/${topicId}_partition_metadata.log` as FilePath;
        console.log(`[Partition] Partition metadata file path: ${this.metadataFilePath}`);

        const metadataFileValidation = ensureFileExists(this.metadataFilePath);
        if (!metadataFileValidation.isValid) {
            console.error("Failed to initialize metadata file:", metadataFileValidation.error);
            process.exit(1);
        }

        const valuesFromMetadata = this.extractDataFromMetadata();
        if (!valuesFromMetadata.success) {
            console.error("Failed to extract metadata:", valuesFromMetadata.errorCode, valuesFromMetadata.error);
            process.exit(1);
        }

        const { logEndOffset, readOffset } = valuesFromMetadata.data;
        this.logEndOffset = logEndOffset;
        this.readOffset = readOffset;
        console.log(`[Partition] Partition ${partitionId} metadata - logEndOffset: ${logEndOffset}, readOffset: ${readOffset}`);
        if (this.logEndOffset < this.readOffset) {
            console.error("Invalid offset state detected. Log end offset is less than read offset.");
            process.exit(1);
        }

        this.buffer = new Queue<Message>();

        this.logHandler = new LogFileHandler({
            label: LOG_FILE_TYPE.PARTITION_BUFFER,
            filePath: this.logFilePath,
            topicId: this.topicId,
            partitionId: this.partitionId
        });

        // Build the partition buffer from the offset and the log file
        const buildResult = this.buildBufferFromLogFile();
        if (!buildResult.success) {
            console.error("Error building partition buffer from log file:", buildResult.errorCode, buildResult.error);
            process.exit(1);
        }
        console.log(`[Partition] Partition ${partitionId} for topic ${topicId} initialized with ${this.buffer.size()} message(s)`);
    }

    // Private methods
    private buildBufferFromLogFile(): Response<boolean> {
        try {
            console.log(`Building partition ${this.partitionId} buffer from log file...`);
            const logFileContent = fs.readFileSync(this.logFilePath, 'utf-8');
            const logs = logFileContent.split("\n").slice(this.readOffset).filter(log => !!log);

            for (const log of logs) {
                // New format: topicId|partitionId|offset|messageId|content
                const [topicId, partitionId, offset, messageId, content] = log.split("|");

                // Only load messages for this specific partition
                this.buffer.enqueue({
                    topicId,
                    messageId: messageId, // messageId is now a string
                    content
                });
            }

            console.log(`[Partition] Partition ${this.partitionId} buffer built with ${this.buffer.size()} message(s)`);
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
            const metadataContent = fs.readFileSync(this.metadataFilePath, 'utf-8');
            const lines = metadataContent.split("\n").filter(line => !!line.trim());

            // Find the line for this specific partition with new format: {topicId}_partition_{partitionId}
            const metadataKey = `${this.topicId}_partition_${this.partitionId}`;
            const partitionLine = lines.find(line => line.startsWith(`${metadataKey}|`));

            if (!partitionLine) {
                // Initialize metadata for this partition if it doesn't exist
                const newEntry = `${metadataKey}|0|0\n`;
                fs.appendFileSync(this.metadataFilePath, newEntry);
                return {
                    success: true,
                    data: {
                        logEndOffset: 0,
                        readOffset: 0
                    }
                };
            }

            const parts = partitionLine.split("|");
            if (parts.length !== 3) {
                return {
                    success: false,
                    errorCode: ERROR_CODES.INVALID_FILE_PATH,
                    error: new Error(`Malformed metadata file: partition entry must contain exactly three values`)
                };
            }

            const [_, logEndOffset, readOffset] = parts;
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
            if (finalOffset !== undefined) {
                this.readOffset = finalOffset;
            } else {
                this.readOffset += 1;
            }

            // Update only this partition's metadata line
            this.updateMetadataFile();

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
            if (finalOffset !== undefined) {
                this.logEndOffset = finalOffset;
            } else {
                this.logEndOffset += 1;
            }

            // Update only this partition's metadata line
            this.updateMetadataFile();

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

    private updateMetadataFile(): void {
        const metadataContent = fs.readFileSync(this.metadataFilePath, 'utf-8');
        const lines = metadataContent.split("\n").filter(line => !!line.trim());

        const metadataKey = `${this.topicId}_partition_${this.partitionId}`;

        // Only replace this partition's metadata line
        const updatedLines = lines.map(line => {
            if (line.startsWith(`${metadataKey}|`)) {
                return `${metadataKey}|${this.logEndOffset}|${this.readOffset}`;
            }
            return line;
        });

        // If this partition's line doesn't exist, add it
        if (!updatedLines.some(line => line.startsWith(`${metadataKey}|`))) {
            updatedLines.push(`${metadataKey}|${this.logEndOffset}|${this.readOffset}`);
        }

        fs.writeFileSync(this.metadataFilePath, updatedLines.join("\n") + "\n");
    }

    // Public methods
    async push(message: Message): Promise<Response<void>> {
        try {
            if (this.buffer.size() >= this.maxBufferSize) {
                return {
                    success: false,
                    errorCode: ERROR_CODES.BUFFER_FULL,
                    error: new Error(`Partition ${this.partitionId} buffer has reached maximum capacity`)
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
                data: undefined
            };
        } catch (error) {
            return {
                success: false,
                errorCode: ERROR_CODES.BUFFER_BUILD_FAILED,
                error: error
            };
        }
    }

    batchExtract(batchSize: number): Response<{ messages: Message[]; startOffset: number; endOffset: number }> {
        try {
            if (this.buffer.isEmpty()) {
                return {
                    success: false,
                    errorCode: ERROR_CODES.BUFFER_EMPTY,
                    error: new Error(`Partition ${this.partitionId} buffer is empty`)
                };
            }

            // Use peekBatch to read messages WITHOUT removing them from buffer
            // Messages will only be removed when consumer calls /commit
            const batch = this.buffer.peekBatch(batchSize);

            // startOffset: current readOffset (last message that was already committed/extracted)
            // endOffset: what the consumer should commit after processing these messages
            //            (this becomes the new readOffset = "last message extracted")
            const startOffset = this.readOffset;
            const endOffset = this.readOffset + batch.length;

            return {
                success: true,
                data: {
                    messages: batch,
                    startOffset,
                    endOffset
                }
            };
        } catch (error) {
            return {
                success: false,
                errorCode: ERROR_CODES.BUFFER_BUILD_FAILED,
                error: error
            };
        }
    }

    commitOffset(offset: number): Response<{ logEndOffset: number; newReadOffset: number }> {
        try {
            // Validate that logEndOffset >= offset
            if (this.logEndOffset < offset) {
                return {
                    success: false,
                    errorCode: ERROR_CODES.INVALID_OFFSET,
                    error: new Error(`Invalid offset: ${offset} exceeds logEndOffset: ${this.logEndOffset}`)
                };
            }

            // Calculate how many messages to remove from buffer
            const messagesToDequeue = offset - this.readOffset;
            if (messagesToDequeue > 0) {
                // Actually remove the committed messages from the buffer
                this.buffer.dequeueBatch(messagesToDequeue);
            }

            // Update readOffset
            const updateResult = this.updateReadOffset(offset);
            if (!updateResult.success) {
                return updateResult as Response<{ logEndOffset: number; newReadOffset: number }>;
            }

            return {
                success: true,
                data: {
                    logEndOffset: this.logEndOffset,
                    newReadOffset: this.readOffset
                }
            };
        } catch (error) {
            return {
                success: false,
                errorCode: ERROR_CODES.UNKNOWN_ERROR,
                error: error
            };
        }
    }
}

export default Partition;
