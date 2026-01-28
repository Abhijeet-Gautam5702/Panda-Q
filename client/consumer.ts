import { BrokerId, ConsumerId, ConsumptionResponse, PartitionId, Response, TopicId } from "./types.js";

/**
 * Consumer client for consuming messages from a Panda-Q broker partition.
 * 
 * The Consumer class provides a simple interface for consuming messages from a specific partition
 * on a Panda-Q broker. It supports both single message consumption and batch consumption,
 * with optional basic authentication.
 * 
 * Each Consumer instance is tied to a specific partition, ensuring ordered message processing
 * from that partition.
 * 
 * @example
 * ```typescript
 * // Create a consumer without authentication
 * const consumer = new Consumer(
 *   "http://localhost:3000",
 *   "brokerId-1",
 *   "topicId-1",
 *   0 // partition ID
 * );
 * 
 * // Create a consumer with basic authentication
 * const secureConsumer = new Consumer(
 *   "http://localhost:3000",
 *   "brokerId-1",
 *   "topicId-1",
 *   0,
 *   "myUsername",
 *   "myPassword"
 * );
 * 
 * // Consume a single message
 * const result = await consumer.consume();
 * if (result.success) {
 *   console.log("Message:", result.data);
 * }
 * 
 * // Consume messages in batch
 * const batchResult = await consumer.batchConsume();
 * if (batchResult.success) {
 *   console.log("Batch:", batchResult.data);
 * }
 * ```
 */
class Consumer {
    private readonly brokerUrl: string;
    private readonly brokerId: BrokerId;
    private readonly consumerId: ConsumerId;
    private readonly topicId: TopicId;
    private readonly partitionId: PartitionId;
    private readonly username?: string;
    private readonly password?: string;

    constructor(
        brokerUrl: string,
        brokerId: BrokerId,
        consumerId: ConsumerId,
        topicId: TopicId,
        partitionId: PartitionId,
        username?: string,
        password?: string
    ) {
        this.brokerUrl = brokerUrl;
        this.brokerId = brokerId;
        this.topicId = topicId;
        this.consumerId = consumerId;
        this.partitionId = partitionId;
        this.username = username;
        this.password = password;
    }

    private async fetchMessage(batch: boolean = false): Promise<Response<any>> {
        try {
            let url = `${this.brokerUrl}/consume/${this.brokerId} /${this.topicId}/${this.partitionId}`;
            if (batch) {
                url += `?b=t`;
            }

            const headers: Record<string, string> = {
                "Content-Type": "application/json"
            };
            if (this.username && this.password) {
                const credentials = Buffer.from(`${this.username}:${this.password}`).toString('base64');
                headers['Authorization'] = `Basic ${credentials}`;
            }

            const response = await fetch(url, {
                method: "GET",
                headers
            });

            if (response.status !== 200) {
                throw new Error(`Failed to consume message: ${response.statusText}`);
            }

            const data = await response.json();
            return {
                success: true,
                data
            };
        } catch (error) {
            console.error(`Failed to consume message: ${error}`);
            return {
                success: false,
                error
            };
        }
    }

    /**
     * Consumes a single message from the partition.
     * 
     * This method fetches one message at a time from the configured partition.
     * Messages are consumed in the order they were produced (FIFO within the partition).
     * If authentication credentials were provided in the constructor, they will be
     * included as a Basic Authorization header.
     * 
     * @returns A Promise that resolves to a Response object containing the consumed message
     * 
     * @example
     * ```typescript
     * const result = await consumer.consume();
     * 
     * if (result.success) {
     *   const message = result.data;
     *   console.log("Consumed message:", message);
     *   // Process the message
     * } else {
     *   console.error("Failed to consume:", result.error);
     * }
     * ```
     * 
     * @throws {Error} Throws if the consumption request fails
     */
    async consume(): Promise<Response<ConsumptionResponse>> {
        try {
            const response = await this.fetchMessage();

            if (!response.success) {
                throw new Error(`Failed to consume message: ${response.error}`);
            }

            return response.data;
        } catch (error) {
            console.error(`Failed to consume message: ${error}`);
            return {
                success: false,
                error
            };
        }
    }

    /**
     * Consumes multiple messages from the partition in a single request.
     * 
     * This method fetches multiple messages at once from the configured partition,
     * which can be more efficient than making multiple single consume() calls.
     * The batch size is determined by the broker's configuration.
     * If authentication credentials were provided in the constructor, they will be
     * included as a Basic Authorization header.
     * 
     * @returns A Promise that resolves to a Response object containing an array of consumed messages
     * 
     * @example
     * ```typescript
     * const result = await consumer.batchConsume();
     * 
     * if (result.success) {
     *   const messages = result.data; // Array of messages
     *   console.log(`Consumed ${messages.length} messages`);
     *   messages.forEach(msg => {
     *     // Process each message
     *     console.log("Message:", msg);
     *   });
     * } else {
     *   console.error("Failed to batch consume:", result.error);
     * }
     * ```
     * 
     * @throws {Error} Throws if the batch consumption request fails
     */
    async batchConsume(): Promise<Response<ConsumptionResponse>> {
        try {
            const response = await this.fetchMessage(true);

            if (!response.success) {
                throw new Error(`Failed to consume message: ${response.error}`);
            }

            return response.data;
        } catch (error) {
            console.error(`Failed to consume message: ${error}`);
            return {
                success: false,
                error
            };
        }
    }

    /**
     * Commits the offset of the last consumed message to the broker.
     * 
     * This method sends the offset of the last consumed message to the broker's commit endpoint
     * using HTTP POST. If authentication credentials were provided in the constructor, they will be
     * included as a Basic Authorization header.
     * 
     * @param offset - The offset of the last consumed message
     * @returns A Promise that resolves to a Response object with success status and data/error
     * 
     * @example
     * ```typescript
     * const result = await consumer.commitOffset(123);
     * 
     * if (result.success) {
     *   console.log("Offset committed successfully:", result.data);
     * } else {
     *   console.error("Failed to commit offset:", result.error);
     * }
     * ```
     * 
     * @throws {Error} Throws if the commit request fails
     */
    async commitOffset(offset: number): Promise<Response<any>> {
        try {
            const payload = {
                brokerId: this.brokerId,
                topicId: this.topicId,
                partitionId: this.partitionId,
                consumerId: this.consumerId,
                offset
            };

            const response = await fetch(`${this.brokerUrl}/commit`, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json"
                },
                body: JSON.stringify(payload)
            });

            if (response.status !== 200) {
                throw new Error(`Failed to commit offset: ${response.statusText}`);
            }

            const data = await response.json();
            return {
                success: true,
                data
            };
        } catch (error) {
            console.error(`Failed to commit offset: ${error}`);
            return {
                success: false,
                error
            };
        }
    }
}

export default Consumer;