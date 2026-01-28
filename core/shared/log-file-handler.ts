import { promises as fs } from 'fs';
import ERROR_CODES from "./error-codes.js";
import { ensureFileExists } from "./utils.js";
import { FilePath, Message, BrokerId, LOG_FILE_TYPE, Response } from "./types.js";
import dotenv from "dotenv";
import getEnv from './env-config.js';
dotenv.config();

// Log File Handler
class LogFileHandler {
    private readonly label: LOG_FILE_TYPE;
    private readonly filePath: FilePath;
    private readonly topicId?: string;
    private readonly partitionId?: number;
    private static readonly brokerId: BrokerId = getEnv().BROKER_ID as BrokerId;

    constructor(config: {
        label: LOG_FILE_TYPE,
        filePath: FilePath,
        topicId?: string,
        partitionId?: number,
    }) {
        // Check if file exists (create if not)
        const fileValidation = ensureFileExists(config.filePath);
        if (!fileValidation.isValid) {
            console.error("Failed to initialize log file handler:", fileValidation.error);
            throw new Error(fileValidation.error);
        }

        this.label = config.label;
        this.filePath = config.filePath;
        this.topicId = config.topicId;
        this.partitionId = config.partitionId;
    }

    private formatLogEntry(message: Message, offset: number): string {
        const stringifiedMsg = typeof message.content === 'string' ? message.content : JSON.stringify(message.content);

        if (this.label === LOG_FILE_TYPE.INGRESS_BUFFER) {
            return [LogFileHandler.brokerId, offset, message.topicId, message.messageId, stringifiedMsg].join("|") + "\n";
        } else if (this.label === LOG_FILE_TYPE.PARTITION_BUFFER) {
            return [this.topicId, this.partitionId, offset, message.messageId, stringifiedMsg].join("|") + "\n";
        }

        return "";
    }

    async append(message: Message, offset: number): Promise<Response<boolean>> {
        try {
            await fs.appendFile(this.filePath, this.formatLogEntry(message, offset), 'utf-8');
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
}

export default LogFileHandler;