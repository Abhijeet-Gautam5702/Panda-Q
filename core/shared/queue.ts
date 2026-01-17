class Queue<T> {
    private queue: T[];

    constructor() {
        this.queue = [];
    }

    enqueue(item: T): void {
        this.queue.push(item);
    }

    dequeue(): T | undefined {
        return this.queue.shift();
    }

    peek(): T | undefined {
        return this.queue[0];
    }

    rear(): T | undefined {
        return this.queue[this.queue.length - 1];
    }

    size(): number {
        return this.queue.length;
    }

    isEmpty(): boolean {
        return this.queue.length === 0;
    }

    clear(): void {
        this.queue = [];
    }
}

export default Queue;