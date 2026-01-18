import IngressBuffer from "./ingress-buffer.ts";
import dotenv from "dotenv";
dotenv.config();

async function main() {
    console.log("Bootstrapping the server...");
    // console.log("INGRESS_LOG_FILE:", process.env.INGRESS_LOG_FILE);
    // console.log("INGRESS_METADATA_FILE:", process.env.INGRESS_METADATA_FILE);
    // Bootstrap the server
    // - Check for the log files in the data storage volume
    // - If log files are present,
    //      - data match: restore the state of the server
    //      - data mismatch: refuse to start at all
    // - If log files are not present, 
    //      initialize the server with the configuration set in the docker file
    // - Start the server
    console.log("Starting the ingress buffer...");
    const ingressBuffer = new IngressBuffer();
    console.log("Ingress buffer started successfully.");

    for (let i = 0; i < 4; i++) {
        await ingressBuffer.push({
            topicId: "1",
            messageId: String(Math.random() + i),
            content: "Hello World--" + Math.random() + "--New Message"
        });
    }
    console.log("Ingress Buffer final state:", ingressBuffer.buffer);
    console.log("Ingress Buffer offset:", ingressBuffer.offset);
    console.log("Ingress Buffer size:", ingressBuffer.buffer.size());
    // Start the HTTP server

    // Start the broker

    process.exit(0);
}

main().catch(console.error);