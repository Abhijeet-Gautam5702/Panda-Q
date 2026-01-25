export type PartitionId = number;
export type TopicId = string;
export type BrokerId = string;

export type Message = {
    topicId: TopicId;
    messageId: number;
    content: string;
}

export type FilePath = string;

export type ValidationResult = {
    isValid: boolean;
    error?: string;
};

export type SuccessResponse<T = any> = {
    success: true;
    data: T;
};

export type ErrorResponse = {
    success: false;
    errorCode: string;
    error: any;
};

export type Response<T = any> = SuccessResponse<T> | ErrorResponse;

export enum LOG_FILE_TYPE {
    INGRESS_BUFFER,
    PARTITION_BUFFER
}
