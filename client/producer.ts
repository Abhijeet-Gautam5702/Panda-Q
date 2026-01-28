import { BrokerId, TopicId, Response } from "./types.js";

/**
 * Producer client for sending messages to a Panda-Q broker.
 * 
 * The Producer class provides a simple interface for publishing messages to a specific topic
 * on a Panda-Q broker. It supports both authenticated and unauthenticated connections.
 * 
 * @example
 * ```typescript
 * // Create a producer without authentication
 * const producer = new Producer(
 *   "broker-1",
 *   "http://localhost:3000",
 *   "orders-topic"
 * );
 * 
 * // Create a producer with basic authentication
 * const secureProducer = new Producer(
 *   "broker-1",
 *   "http://localhost:3000",
 *   "orders-topic",
 *   "myUsername",
 *   "myPassword"
 * );
 * 
 * // Send a message
 * const result = await producer.produce({
 *   messageId: 1,
 *   content: { productId: "abc", quantity: 5 }
 * });
 * ```
 */
class Producer {
    private readonly brokerId: BrokerId;
    private readonly brokerUrl: string;
    private readonly topicId: TopicId;
    private readonly username?: string;
    private readonly password?: string;

    constructor(
        brokerId: BrokerId,
        brokerUrl: string,
        topicId: TopicId,
        username?: string,
        password?: string
    ) {
        this.brokerId = brokerId;
        this.brokerUrl = brokerUrl;
        this.topicId = topicId;
        this.username = username;
        this.password = password;
    }

    /**
     * Produces (sends) a message to the configured topic on the broker.
     * 
     * This method sends the message to the broker's ingress endpoint using HTTP POST.
     * If authentication credentials were provided in the constructor, they will be
     * included as a Basic Authorization header.
     * 
     * @param message - The message to send to the topic. Must contain at least a key and value.
     * @returns A Promise that resolves to a Response object with success status and data/error
     * 
     * @example
     * ```typescript
     * const result = await producer.produce({
     *   messageId: 1,
     *   content: { name: "John", email: "john@example.com" }
     * });
     * 
     * if (result.success) {
     *   console.log("Message sent successfully:", result.data);
     * } else {
     *   console.error("Failed to send message:", result.error);
     * }
     * ```
     * 
     * @throws {Error} Throws if the message is null or undefined
     */
    async produce(message: {
        messageId: string;
        content: any;
    }): Promise<Response<any>> {
        if (!message) {
            throw new Error("Invalid Message");
        }
        // TODO: Validate the topicId of the producer exists in the TPC or not?
        // Create an endpoint to check for TPC map
        try {
            const content = message.content;
            const payload = {
                brokerId: this.brokerId,
                message: {
                    topicId: this.topicId,
                    messageId: message.messageId,
                    content: typeof content === 'string' ? content : JSON.stringify(content)
                }
            };

            const headers: Record<string, string> = {
                "Content-Type": "application/json"
            };

            if (this.username && this.password) {
                const credentials = Buffer.from(`${this.username}:${this.password}`).toString('base64');
                headers['Authorization'] = `Basic ${credentials}`;
            }

            const response = await fetch(`${this.brokerUrl}/ingress/${this.topicId}`, {
                method: "POST",
                headers,
                body: JSON.stringify(payload)
            });

            if (response.status !== 200) {
                throw new Error(`Failed to produce message: ${response.statusText}`);
            }

            const data = await response.json();
            return {
                success: true,
                data
            };
        } catch (error) {
            console.error(`Failed to produce message: ${error}`);
            return {
                success: false,
                error
            };
        }
    }
}

export default Producer;