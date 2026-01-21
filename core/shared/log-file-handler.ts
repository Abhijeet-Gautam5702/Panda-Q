import { promises as fs } from 'fs';
import ERROR_CODES from "./error-codes.ts";
import { ensureFileExists } from "./utils.ts";
import { FilePath, Message, BrokerId, LOG_FILE_TYPE, Response } from "./types.ts";
import dotenv from "dotenv";
dotenv.config();

// Log File Handler
class LogFileHandler {
    private readonly label: LOG_FILE_TYPE;
    private readonly filePath: FilePath;
    private static readonly dataStorageVolume: FilePath = process.env.DATA_STORAGE_VOLUME as FilePath;
    private static readonly brokerId: BrokerId = process.env.BROKER_ID as BrokerId;

    constructor(config: {
        label: LOG_FILE_TYPE,
        filePath: FilePath,
    }) {
        // Check if file exists (create if not)
        const fileValidation = ensureFileExists(config.filePath);
        if (!fileValidation.isValid) {
            console.error("Failed to initialize log file handler:", fileValidation.error);
            throw new Error(fileValidation.error);
        }

        this.label = config.label;
        this.filePath = config.filePath;
    }

    private formatLogEntry(message: Message, offset: number): string {
        switch (this.label) {
            case LOG_FILE_TYPE.INGRESS_BUFFER:
                const stringifiedMsg = typeof message.content === 'string' ? message.content : JSON.stringify(message.content);
                return [LogFileHandler.brokerId, offset, message.topicId, message.messageId, stringifiedMsg].join("|") + "\n";
            default:
                return "";
        }
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