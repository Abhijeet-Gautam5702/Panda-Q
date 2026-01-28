import fs from "fs";
import path from "path";
import { TopicId, PartitionId, ConsumerId } from "./types.js";
import getEnv from "./env-config.js";


const TPC_LOG_FILE = "TPC.log";

export function getTPCLogPath(): string {
    const dataDir = getEnv().DATA_STORAGE_VOLUME || "pandaq-data";
    return path.join(process.cwd(), dataDir, TPC_LOG_FILE);
}

export function serializeTPCMap(tpcMap: Map<TopicId, Map<PartitionId, ConsumerId>>): string {
    const lines: string[] = [];

    for (const [topicId, partitionMap] of tpcMap) {
        for (const [partitionId, consumerId] of partitionMap) {
            lines.push(`${topicId}|${partitionId}|${consumerId}`);
        }
    }

    return lines.join("\n") + "\n";
}

export function deserializeTPCMap(content: string): Map<TopicId, Map<PartitionId, ConsumerId>> {
    const tpcMap = new Map<TopicId, Map<PartitionId, ConsumerId>>();

    const lines = content.split("\n").filter(line => line.trim());

    for (const line of lines) {
        const [topicId, partitionIdStr, consumerId] = line.split("|");
        const partitionId = parseInt(partitionIdStr);

        if (!tpcMap.has(topicId)) {
            tpcMap.set(topicId, new Map<PartitionId, ConsumerId>());
        }

        tpcMap.get(topicId)!.set(partitionId, consumerId || "");
    }

    return tpcMap;
}

export function writeTPCLog(tpcMap: Map<TopicId, Map<PartitionId, ConsumerId>>): void {
    const logPath = getTPCLogPath();
    const content = serializeTPCMap(tpcMap);
    fs.writeFileSync(logPath, content);
    console.log(`[TPCHelper] TPC Map written to ${logPath}`);
}

export function readTPCLog(): Map<TopicId, Map<PartitionId, ConsumerId>> | null {
    const logPath = getTPCLogPath();

    if (!fs.existsSync(logPath)) {
        console.log(`[TPCHelper] TPC.log not found at ${logPath}`);
        return null;
    }

    const content = fs.readFileSync(logPath, "utf-8");
    const tpcMap = deserializeTPCMap(content);
    console.log(`[TPCHelper] TPC Map loaded from ${logPath} with ${tpcMap.size} topic(s)`);
    return tpcMap;
}

export function tpcLogExists(): boolean {
    return fs.existsSync(getTPCLogPath());
}
