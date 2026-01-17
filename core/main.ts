import IngressBuffer from "./ingress-buffer";

function main() {
    // Bootstrap the server
    // - Check for the log files in the data storage volume
    // - If log files are present,
    //      - data match: restore the state of the server
    //      - data mismatch: refuse to start at all
    // - If log files are not present, 
    //      initialize the server with the configuration set in the docker file
    // - Start the server
    const ingressBuffer = new IngressBuffer(
        0,
        "/tmp/ingress-buffer.log"
    );

    // Start the HTTP server


    // Start the broker

}

main();