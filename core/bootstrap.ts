import fs from "fs";
import path from "path";
import { internalTPCMap } from "./main.js";
import { readTPCLog, writeTPCLog, tpcLogExists } from "./shared/tpc-helper.js";

interface TopicConfig {
    id: string;
    partitions: number;
}

interface BrokerConfig {
    brokerId: string;
    topics: TopicConfig[];
    reboot: boolean;
}

/**
 * Bootstrap utilities for initializing the Panda-Q data storage
 */
export class Bootstrap {
    private static readonly CONFIG_FILE = "pandaq-config.json";
    private static readonly DATA_DIR = "pandaq-data";

    /**
     * Parse the pandaq-config.json file
     */
    static parseConfig(): BrokerConfig {
        console.log("[Bootstrap] Parsing configuration file...");
        const configPath = path.join(process.cwd(), Bootstrap.CONFIG_FILE);

        if (!fs.existsSync(configPath)) {
            throw new Error(`Configuration file not found: ${configPath}`);
        }

        const content = fs.readFileSync(configPath, 'utf-8');

        try {
            const config = JSON.parse(content);

            // Validate required fields
            if (!config.brokerId || typeof config.brokerId !== 'string') {
                throw new Error("Missing or invalid 'brokerId' in config");
            }

            if (!Array.isArray(config.topics)) {
                throw new Error("Missing or invalid 'topics' array in config");
            }

            // Transform topics to expected format
            const topics: TopicConfig[] = config.topics.map((topic: any) => {
                if (!topic.id || typeof topic.id !== 'string') {
                    throw new Error("Each topic must have a valid 'id' string");
                }
                if (!topic.partitions || typeof topic.partitions !== 'number') {
                    throw new Error(`Topic '${topic.id}' must have a valid 'partitions' number`);
                }
                return {
                    id: topic.id,
                    partitions: topic.partitions
                };
            });

            const result: BrokerConfig = {
                brokerId: config.brokerId,
                topics,
                reboot: config.reboot === true
            };

            console.log(`[Bootstrap] Parsed config - Broker: ${result.brokerId}, Topics: ${result.topics.length}, Reboot: ${result.reboot}`);
            return result;
        } catch (error) {
            if (error instanceof SyntaxError) {
                throw new Error(`Invalid JSON in config file: ${error.message}`);
            }
            throw error;
        }
    }

    /**
     * Populate the internal TPC Map from TPC.log (if exists) or from config
     */
    static populateTPCMap(config: BrokerConfig): void {
        console.log("[Bootstrap] Populating TPC Map...");

        // Try to load from TPC.log first
        if (tpcLogExists()) {
            console.log("[Bootstrap] Found existing TPC.log, loading from file...");
            const loadedMap = readTPCLog();
            if (loadedMap) {
                // Copy the loaded map to the global internalTPCMap
                for (const [topicId, partitionMap] of loadedMap) {
                    internalTPCMap.set(topicId, partitionMap);
                }
                console.log(`[Bootstrap] TPC Map restored from TPC.log with ${internalTPCMap.size} topic(s)`);
                return;
            }
        }

        // Fall back to populating from config if TPC.log doesn't exist
        console.log("[Bootstrap] No TPC.log found, populating from config...");
        for (const topic of config.topics) {
            const partitionMap = new Map<number, string>();
            for (let i = 0; i < topic.partitions; i++) {
                partitionMap.set(i, ""); // Empty consumer ID initially
            }
            internalTPCMap.set(topic.id, partitionMap);
        }

        // Write the new TPC Map to TPC.log
        writeTPCLog(internalTPCMap);
        console.log(`[Bootstrap] TPC Map populated with ${internalTPCMap.size} topic(s) and saved to TPC.log`);
    }

    /**
     * Initialize the data directory structure
     */
    static initializeDataDirectory(config: BrokerConfig): void {
        console.log("[Bootstrap] Initializing data directory structure...");
        const dataDir = path.join(process.cwd(), Bootstrap.DATA_DIR);

        // Create main data directory
        if (!fs.existsSync(dataDir)) {
            fs.mkdirSync(dataDir, { recursive: true });
            console.log(`[Bootstrap] Created data directory: ${dataDir}`);
        }

        // Create ingress log and metadata files
        this.initializeFile(path.join(dataDir, 'ingress.log'));
        this.initializeFile(path.join(dataDir, 'ingress_metadata.log'), 'ingress|0|0\n');

        // Create config log file
        this.initializeConfigLog(dataDir, config);

        // Create topic directories and files
        const topicsDir = path.join(dataDir, 'topics');
        if (!fs.existsSync(topicsDir)) {
            fs.mkdirSync(topicsDir, { recursive: true });
        }

        for (const topic of config.topics) {
            this.initializeTopic(topicsDir, topic);
        }

        console.log("[Bootstrap] Data directory initialization complete");
    }

    /**
     * Validate existing data directory against config
     */
    static validateDataDirectory(config: BrokerConfig): void {
        console.log("[Bootstrap] Validating data directory consistency...");
        const dataDir = path.join(process.cwd(), Bootstrap.DATA_DIR);

        if (!fs.existsSync(dataDir)) {
            throw new Error("Data directory does not exist. Run initialization first.");
        }

        // Validate config.log
        this.validateConfigLog(dataDir, config);

        // Validate topics
        const topicsDir = path.join(dataDir, 'topics');
        for (const topic of config.topics) {
            this.validateTopic(topicsDir, topic);
        }

        console.log("[Bootstrap] Data directory validation passed");
    }

    /**
     * Bootstrap: either initialize or validate
     */
    static async bootstrap(): Promise<BrokerConfig> {
        console.log("[Bootstrap] Starting bootstrap process...");

        const config = this.parseConfig();
        const dataDir = path.join(process.cwd(), Bootstrap.DATA_DIR);

        // Handle reboot flag
        if (config.reboot) {
            console.log("[Bootstrap] Reboot flag enabled - deleting existing data directory...");
            if (fs.existsSync(dataDir)) {
                fs.rmSync(dataDir, { recursive: true, force: true });
                console.log("[Bootstrap] Data directory deleted successfully");
            }
        }

        if (fs.existsSync(dataDir)) {
            console.log("[Bootstrap] Data directory exists, validating consistency...");
            this.validateDataDirectory(config);
        } else {
            console.log("[Bootstrap] Data directory not found, initializing...");
            this.initializeDataDirectory(config);
        }

        // Populate TPC Map after initialization/validation
        this.populateTPCMap(config);

        return config;
    }

    // Helper methods
    private static initializeFile(filePath: string, defaultContent: string = ''): void {
        if (!fs.existsSync(filePath)) {
            fs.writeFileSync(filePath, defaultContent);
            console.log(`[Bootstrap] Created file: ${filePath}`);
        }
    }

    private static initializeConfigLog(dataDir: string, config: BrokerConfig): void {
        const configPath = path.join(dataDir, 'config.log');
        const lines: string[] = [];

        for (const topic of config.topics) {
            lines.push(`topic_config|${topic.id}|${topic.partitions}`);
        }

        fs.writeFileSync(configPath, lines.join('\n') + '\n');
        console.log(`[Bootstrap] Created config.log with ${config.topics.length} topic(s)`);
    }

    private static initializeTopic(topicsDir: string, topic: TopicConfig): void {
        const topicDir = path.join(topicsDir, `topic_${topic.id}`);

        if (!fs.existsSync(topicDir)) {
            fs.mkdirSync(topicDir, { recursive: true });
        }

        // Create partition log files
        for (let i = 0; i < topic.partitions; i++) {
            this.initializeFile(path.join(topicDir, `partition_${i}.log`));
        }

        // Create topic-specific partition metadata file
        this.initializeFile(path.join(topicDir, `${topic.id}_partition_metadata.log`));

        console.log(`[Bootstrap] Initialized topic ${topic.id} with ${topic.partitions} partition(s)`);
    }

    private static validateConfigLog(dataDir: string, config: BrokerConfig): void {
        const configPath = path.join(dataDir, 'config.log');

        if (!fs.existsSync(configPath)) {
            throw new Error("config.log not found in data directory");
        }

        const content = fs.readFileSync(configPath, 'utf-8');
        const lines = content.split('\n').filter(line => line.trim());

        if (lines.length !== config.topics.length) {
            throw new Error(`Config mismatch: Expected ${config.topics.length} topics, found ${lines.length} in config.log`);
        }

        for (const topic of config.topics) {
            const expectedLine = `topic_config|${topic.id}|${topic.partitions}`;
            if (!lines.includes(expectedLine)) {
                throw new Error(`Config mismatch: Topic ${topic.id} configuration doesn't match. Expected: ${expectedLine}`);
            }
        }
    }

    private static validateTopic(topicsDir: string, topic: TopicConfig): void {
        const topicDir = path.join(topicsDir, `topic_${topic.id}`);

        if (!fs.existsSync(topicDir)) {
            throw new Error(`Topic directory missing: topic_${topic.id}`);
        }

        // Validate partition log files
        for (let i = 0; i < topic.partitions; i++) {
            const partitionFile = path.join(topicDir, `partition_${i}.log`);
            if (!fs.existsSync(partitionFile)) {
                throw new Error(`Partition file missing: topic_${topic.id}/partition_${i}.log`);
            }
        }

        // Validate metadata file
        const metadataFile = path.join(topicDir, `${topic.id}_partition_metadata.log`);
        if (!fs.existsSync(metadataFile)) {
            throw new Error(`Partition metadata file missing: topic_${topic.id}/${topic.id}_partition_metadata.log`);
        }
    }
}
