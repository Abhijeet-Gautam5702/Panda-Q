import fs from 'fs';

export class MetricsCollector {
    constructor() {
        this.latencies = [];
    }

    recordLatency(latency) {
        this.latencies.push(latency);
    }

    calculatePercentiles() {
        if (this.latencies.length === 0) return { p50: 0, p95: 0, p99: 0 };

        // Sort latencies numerically
        this.latencies.sort((a, b) => a - b);

        const p50Index = Math.floor(this.latencies.length * 0.50);
        const p95Index = Math.floor(this.latencies.length * 0.95);
        const p99Index = Math.floor(this.latencies.length * 0.99);

        return {
            p50: this.latencies[p50Index],
            p95: this.latencies[p95Index],
            p99: this.latencies[p99Index],
            count: this.latencies.length,
            min: this.latencies[0],
            max: this.latencies[this.latencies.length - 1]
        };
    }

    saveToFile(filename = 'metric.js') {
        const stats = this.calculatePercentiles();
        const content = `// Metrics generated at ${new Date().toISOString()}
const metrics = ${JSON.stringify(stats, null, 2)};
module.exports = metrics;
`;
        fs.writeFileSync(filename, content);
        console.log(`Metrics saved to ${filename}`);
        return stats;
    }
}
