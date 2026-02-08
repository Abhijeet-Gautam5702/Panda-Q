import express, { Request, Response } from 'express';
import Broker from './broker.js';
import { Message, TopicId } from './shared/types.js';
import { internalTPCMap } from './main.js';
import { writeTPCLog } from './shared/tpc-helper.js';
import ERROR_CODES from './shared/error-codes.js';

/**
 * HTTP Server for Panda-Q Broker
 * 
 * Exposes REST API endpoints for producers and consumers to interact with the broker.
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
        this.app.use((req, _, next) => {
            console.log(`[ACCESS] ${req.method} ${req.path}`);
            next();
        });
    }

    private setupRoutes(): void {
        // Consumer registration: POST /register/:topicId
        this.app.post('/register/:topicId', async (req, res) => {
            try {
                const { topicId } = req.params;
                const { brokerId, consumerId } = req.body;

                if (!topicId || !brokerId || !consumerId) {
                    return res.status(400).json({
                        success: false,
                        error: 'Invalid consumer registration format. Expected { topicId, brokerId, consumerId }'
                    });
                }

                // Register consumer with broker
                const result = await this.broker.registerConsumer(topicId, consumerId);

                if (!result.success) {
                    return res.status(500).json(result);
                }

                res.status(200).json({
                    success: true,
                    data: {
                        topicId,
                        brokerId,
                        consumerId,
                        partitionId: result.data.partitionId,
                        timestamp: new Date().toISOString()
                    }
                });
            } catch (error) {
                console.error('[SERVER] Error in /register endpoint:', error);
                res.status(500).json({
                    success: false,
                    error: 'Internal server error'
                });
            }
        })

        // Producer endpoint: POST /ingress/:topicId
        this.app.post('/ingress/:topicId', async (req, res) => {
            try {
                const { topicId } = req.params;
                const { brokerId, message } = req.body;

                // TODO: Validate the brokerId and topicId exists or not

                if (!message || !message.messageId || !message.content) {
                    return res.status(400).json({
                        success: false,
                        error: 'Invalid message format. Expected { messageId, content }'
                    });
                }

                // Create message object matching internal format
                const internalMessage: Message = {
                    topicId: topicId as TopicId,
                    messageId: String(message.messageId), // Ensure messageId is string
                    content: message.content
                };

                // Push message to ingress buffer
                console.log("DEBUG | SERVER | pushing into ingress buffer")
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

                console.log(`[SERVER] Consume request - Broker: ${brokerId}, Topic: ${topicId}, Partition: ${partitionId}, Batch: ${isBatch}`);

                // Get topic from broker
                const topic = this.broker.getTopic(topicId);
                if (!topic) {
                    return res.status(404).json({
                        success: false,
                        error: `Topic ${topicId} not found`
                    });
                }

                // Get partition from topic
                const partitionIdNum = Number(partitionId);
                const partition = topic.getPartition(partitionIdNum);
                if (!partition) {
                    return res.status(404).json({
                        success: false,
                        error: `Partition ${partitionId} not found in topic ${topicId}`
                    });
                }

                // Extract messages from partition buffer
                const batchSize = isBatch ? 100 : 1;
                const extractResult = partition.batchExtract(batchSize);

                if (!extractResult.success) {
                    // Buffer is empty - return empty result (not an error)
                    if (extractResult.errorCode === ERROR_CODES.BUFFER_EMPTY) {
                        return res.status(200).json({
                            success: true,
                            data: {
                                messages: isBatch ? [] : null,
                                count: 0,
                                startOffset: 0,
                                endOffset: 0
                            }
                        });
                    }
                    return res.status(500).json(extractResult);
                }

                const { messages, startOffset, endOffset } = extractResult.data;

                res.status(200).json({
                    success: true,
                    data: {
                        messages: isBatch ? messages : (messages[0] || null),
                        count: messages.length,
                        startOffset,
                        endOffset  // Consumer should commit this offset after processing
                    }
                });
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

                if (typeof offset !== 'number' || !topicId || !consumerId || partitionId === undefined) {
                    return res.status(400).json({
                        success: false,
                        error: 'Invalid commit format. Expected { brokerId, topicId, partitionId, consumerId, offset }'
                    });
                }

                // Validate topic exists in TPC Map
                const partitionMap = internalTPCMap.get(topicId);
                if (!partitionMap) {
                    return res.status(404).json({
                        success: false,
                        error: `Topic ${topicId} not found`
                    });
                }

                // Validate partition exists
                const partitionIdNum = Number(partitionId);
                if (!partitionMap.has(partitionIdNum)) {
                    return res.status(404).json({
                        success: false,
                        error: `Partition ${partitionId} not found in topic ${topicId}`
                    });
                }

                // Get the partition from broker
                const topic = this.broker.getTopic(topicId);
                if (!topic) {
                    return res.status(404).json({
                        success: false,
                        error: `Topic ${topicId} not found in broker`
                    });
                }

                const partition = topic.getPartition(partitionIdNum);
                if (!partition) {
                    return res.status(404).json({
                        success: false,
                        error: `Partition ${partitionIdNum} not found in topic ${topicId}`
                    });
                }

                // Commit offset - validates logEndOffset >= offset and updates readOffset
                const commitResult = partition.commitOffset(offset);
                if (!commitResult.success) {
                    return res.status(400).json(commitResult);
                }

                // Update TPC Map with consumer assignment
                partitionMap.set(partitionIdNum, consumerId);

                // Persist TPC Map to TPC.log
                writeTPCLog(internalTPCMap);

                console.log(`[SERVER] Commit offset - Topic: ${topicId}, Partition: ${partitionId}, Consumer: ${consumerId}, Offset: ${offset}`);

                res.status(200).json({
                    success: true,
                    data: {
                        committed: true,
                        offset,
                        topicId,
                        partitionId: partitionIdNum,
                        consumerId,
                        logEndOffset: commitResult.data.logEndOffset,
                        newReadOffset: commitResult.data.newReadOffset,
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
        });
    }
}

export default Server;
