import Consumer from '../dist/client/consumer.js'
import fs from 'fs'



async function main() {
    const pandaConsumer = new Consumer(
        "http://localhost:3000",
        "broker_1",
        "nasal-consumer",
        "nasal"
    )
    setTimeout(async () => {
        const response = await pandaConsumer.batchConsume();
        if (response.success) {
            console.log(response.data?.length)
            fs.writeFileSync('data.txt', JSON.stringify(response.data))
        }
    }, 5000)
}

main();