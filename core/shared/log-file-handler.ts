import { FilePath, Message } from "../ingress-buffer.ts";
import { promises as fs } from 'fs';
import ERROR_CODES from "./error-codes.ts";
import { BrokerId } from "../broker.ts";
import dotenv from "dotenv";
dotenv.config();

// Types
export enum LOG_FILE_TYPE {
    INGRESS_BUFFER,
    PARTITION_BUFFER
}

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

    async append(message: Message, offset: number): Promise<boolean | string> {
        try {
            await fs.appendFile(this.filePath, this.formatLogEntry(message, offset), 'utf-8');
            return true;
        } catch (error) {
            console.log("Error appending log entry:", error);
            return ERROR_CODES.LOG_FILE_APPEND_FAILED;
        }
    }
}

export default LogFileHandler;