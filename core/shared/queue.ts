class Queue<T> {
    private queue: Map<number, T>;
    private frontOffset: number;
    private rearOffset: number;

    constructor() {
        this.queue = new Map();
        this.frontOffset = 0;
        this.rearOffset = 0;
    }

    enqueue(item: T): void {
        this.queue.set(this.rearOffset, item);
        this.rearOffset++;

        // Constraint: rearOffset >= frontOffset (always maintained by increment)
        // Constraint: rearOffset >= 0 (always maintained as it only increments from 0)
    }

    dequeue(): T | undefined {
        if (this.isEmpty()) {
            return undefined;
        }

        const item = this.queue.get(this.frontOffset);
        this.queue.delete(this.frontOffset);
        this.frontOffset++;

        // Constraint: frontOffset >= 0 (always maintained as it starts at 0 and only increments)
        // Constraint: frontOffset <= rearOffset (maintained by isEmpty check)

        // Reset offsets when queue becomes empty to prevent unbounded growth
        if (this.isEmpty()) {
            this.frontOffset = 0;
            this.rearOffset = 0;
        }

        return item;
    }

    peek(): T | undefined {
        return this.queue.get(this.frontOffset);
    }

    rear(): T | undefined {
        return this.queue.get(this.rearOffset - 1);
    }

    size(): number {
        return this.rearOffset - this.frontOffset;
    }

    isEmpty(): boolean {
        return this.rearOffset === this.frontOffset;
    }

    clear(): void {
        this.queue.clear();

        // Constraint: Reset both offsets to 0 to maintain frontOffset >= 0, rearOffset >= 0, and frontOffset <= rearOffset
        this.frontOffset = 0;
        this.rearOffset = 0;
    }
}

export default Queue;