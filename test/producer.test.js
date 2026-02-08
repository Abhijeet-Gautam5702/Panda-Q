import Producer from '../dist/client/producer.js';

const pandaProducer = new Producer(
    "broker_1",
    "http://localhost:3000",
    "nasal"
)


async function main() {
    const names = Array.from({ length: 10 }, () => Math.random().toString(36).substring(2, 9));
    for (let i = 100; i < 200; i++) {
        const idx = (Math.ceil(Math.random() * 10) % names.length);
        const message = {
            messageId: String(i),
            content: {
                timestamp: new Date().toISOString(),
                body: {
                    name: names[idx],
                    age: Math.random() * 100 / 100
                }
            }
        }

        const response = await pandaProducer.produce(message)
        if (response.success) {
            console.log(`SUCCESS | Message:${i} Produced Successfully`, response.data)
        }
        else {
            console.log(`FAILED | Message:${i} Failed!`, response.error)
        }
    }
}

main();
