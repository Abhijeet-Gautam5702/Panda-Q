import IngressBuffer from "./ingress-buffer.ts";
import dotenv from "dotenv";
import ERROR_CODES from "./shared/error-codes.ts";
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
        const pushResult = await ingressBuffer.push({
            topicId: "1",
            messageId: String(Math.random() + i),
            content: "Msg --" + Math.random() + "--" + new Date().toLocaleDateString()
        });
        if (pushResult == ERROR_CODES.INGRESS_BUFFER_FULL) {
            console.log("Ingress Buffer full.")
            break;
        }
    }
    console.log("Ingress Buffer final state:", ingressBuffer.buffer);
    console.log("Ingress Buffer log-end offset:", ingressBuffer.logEndOffset);
    console.log("Ingress Buffer read offset:", ingressBuffer.readOffset);
    console.log("Lag B/W Broker Instance & Ingress Buffer:", ingressBuffer.logEndOffset - ingressBuffer.readOffset);
    console.log("Ingress Buffer size:", ingressBuffer.buffer.size());
    // Start the HTTP server

    // Start the broker

    process.exit(0);
}

main().catch(console.error);