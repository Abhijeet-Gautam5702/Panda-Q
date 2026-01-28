export type BrokerId = string;
export type TopicId = string;
export type PartitionId = string;
export type ConsumerId = string;
export type Message = {
    topicId: TopicId;
    messageId: string;
    content: string;
}

type ErrorResponse = {
    success: false;
    error: any;
};
type SuccessResponse<T = any> = {
    success: true;
    data: T;
};
export type Response<T = any> = SuccessResponse<T> | ErrorResponse;

export type ConsumptionResponse = {
    message: Message | Message[];
    offset: number;
}