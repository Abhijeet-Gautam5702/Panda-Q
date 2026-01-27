import express, { Request, Response } from 'express';
import Broker from './broker.ts';
import { Message, TopicId, BrokerId, PartitionId } from './shared/types.ts';

/**
 * HTTP Server for Panda-Q Broker
 * 
 * Exposes REST API endpoints for producers and consumers to interact with the broker.
 * Supports basic authentication for secure communication.
 */
class Server {
    private readonly app: express.Application;
    private readonly broker: Broker;
    private readonly port: number;

    constructor(broker: Broker, port: number = 3000) {
        this.broker = broker;
        this.port = port;
        this.app = express();
        this.setupMiddleware();
        this.setupRoutes();
    }

    private setupMiddleware(): void {
        this.app.use(express.json());
        // Request Access Logs
        this.app.use((req, res, next) => {
            console.log(`[SERVER] ${req.method} ${req.path}`);
            next();
        });
    }

    private setupRoutes(): void {
        // Producer endpoint: POST /ingress/:topicId
        this.app.post('/ingress/:topicId', async (req, res) => {
            try {
                const { topicId } = req.params;
                const { brokerId, message } = req.body;

                if (!message || !message.messageId || !message.content) {
                    return res.status(400).json({
                        success: false,
                        error: 'Invalid message format. Expected { messageId, content }'
                    });
                }

                // Create message object matching internal format
                const internalMessage: Message = {
                    topicId: topicId as TopicId,
                    messageId: message.messageId,
                    content: message.content
                };

                // Push message to ingress buffer
                const result = await this.broker.ingressBuffer.push(internalMessage);

                if (!result.success) {
                    return res.status(500).json(result);
                }

                res.status(200).json({
                    success: true,
                    data: {
                        messageId: message.messageId,
                        topicId,
                        timestamp: new Date().toISOString()
                    }
                });
            } catch (error) {
                console.error('[SERVER] Error in /ingress endpoint:', error);
                res.status(500).json({
                    success: false,
                    error: 'Internal server error'
                });
            }
        });

        // Consumer endpoint: GET /consume/:brokerId/:topicId/:partitionId
        this.app.get('/consume/:brokerId/:topicId/:partitionId', async (req, res) => {
            try {
                const { brokerId, topicId, partitionId } = req.params;
                const { b } = req.query; // batch flag

                const isBatch = b === 't' || b === 'true';

                // TODO: Implement actual consumption logic from partition
                // For now, return a placeholder response
                console.log(`[SERVER] Consume request - Broker: ${brokerId}, Topic: ${topicId}, Partition: ${partitionId}, Batch: ${isBatch}`);

                // Placeholder response
                const response = {
                    success: true,
                    data: {
                        message: isBatch ? [] : null,
                        offset: 0
                    }
                };

                res.json(response);
            } catch (error) {
                console.error('[SERVER] Error in /consume endpoint:', error);
                res.status(500).json({
                    success: false,
                    error: 'Internal server error'
                });
            }
        });

        // Commit offset endpoint: POST /commit
        this.app.post('/commit', async (req, res) => {
            try {
                const { brokerId, topicId, partitionId, consumerId, offset } = req.body;

                if (typeof offset !== 'number') {
                    return res.status(400).json({
                        success: false,
                        error: 'Invalid offset value'
                    });
                }

                // TODO: Implement actual offset commit logic
                console.log(`[SERVER] Commit offset - Broker: ${brokerId}, Topic: ${topicId}, Partition: ${partitionId}, Consumer: ${consumerId}, Offset: ${offset}`);

                res.json({
                    success: true,
                    data: {
                        committed: true,
                        offset,
                        timestamp: new Date().toISOString()
                    }
                });
            } catch (error) {
                console.error('[SERVER] Error in /commit endpoint:', error);
                res.status(500).json({
                    success: false,
                    error: 'Internal server error'
                });
            }
        });
    }

    start(): void {
        this.app.listen(this.port, () => {
            console.log(`[SERVER] Panda-Q HTTP Server listening on port ${this.port}`);
            console.log(`[SERVER] Endpoints available:`);
            console.log(`[SERVER]   - POST /ingress/:topicId`);
            console.log(`[SERVER]   - GET /consume/:brokerId/:topicId/:partitionId`);
            console.log(`[SERVER]   - POST /commit`);
        });
    }
}

export default Server;
