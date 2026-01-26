import fs from "fs";
import path from "path";

interface TopicConfig {
    id: string;
    name: string;
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
    private static readonly CONFIG_FILE = "mock-config.txt";
    private static readonly DATA_DIR = "pandaq-data";

    /**
     * Parse the mock-config.txt file
     */
    static parseConfig(): BrokerConfig {
        console.log("[Bootstrap] Parsing configuration file...");
        const configPath = path.join(process.cwd(), Bootstrap.CONFIG_FILE);

        if (!fs.existsSync(configPath)) {
            throw new Error(`Configuration file not found: ${configPath}`);
        }

        const content = fs.readFileSync(configPath, 'utf-8');
        const lines = content.split('\n');

        let brokerId = '';
        const topics: TopicConfig[] = [];
        let currentTopic: Partial<TopicConfig> | null = null;
        let reboot = false;

        for (const line of lines) {
            const trimmed = line.trim();

            // Parse broker ID
            if (trimmed.startsWith('ID:') && !currentTopic) {
                brokerId = trimmed.split(':')[1].trim();
            }

            // Parse reboot flag
            if (trimmed.startsWith('reboot:')) {
                const value = trimmed.split(':')[1].trim().toLowerCase();
                reboot = value === 'true';
            }

            // Start of a new topic
            if (trimmed.startsWith('- ID:')) {
                if (currentTopic && currentTopic.id && currentTopic.name && currentTopic.partitions) {
                    topics.push(currentTopic as TopicConfig);
                }
                currentTopic = { id: trimmed.split(':')[1].trim() };
            }
            // Parse topic name
            else if (trimmed.startsWith('name:') && currentTopic) {
                currentTopic.name = trimmed.split(':')[1].trim();
            }
            // Parse topic partitions
            else if (trimmed.startsWith('partitions:') && currentTopic) {
                currentTopic.partitions = parseInt(trimmed.split(':')[1].trim());
            }
        }

        // Add last topic
        if (currentTopic && currentTopic.id && currentTopic.name && currentTopic.partitions) {
            topics.push(currentTopic as TopicConfig);
        }

        console.log(`[Bootstrap] Parsed config - Broker: ${brokerId}, Topics: ${topics.length}, Reboot: ${reboot}`);
        return { brokerId, topics, reboot };
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
            lines.push(`topic_config|${topic.id}|${topic.name}|${topic.partitions}`);
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

        console.log(`[Bootstrap] Initialized topic ${topic.name} (${topic.id}) with ${topic.partitions} partition(s)`);
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
            const expectedLine = `topic_config|${topic.id}|${topic.name}|${topic.partitions}`;
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
