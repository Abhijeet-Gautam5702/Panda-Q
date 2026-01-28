let envConfig: any = null;

const setupEnv = () => {
    envConfig = {
        PORT: process.env.PORT || 3000,
        INGRESS_LOG_FILE: process.env.INGRESS_LOG_FILE,
        INGRESS_METADATA_FILE: process.env.INGRESS_METADATA_FILE,
        DATA_STORAGE_VOLUME: process.env.DATA_STORAGE_VOLUME,
        BROKER_ID: process.env.BROKER_ID
    }
}

const getEnv = () => {
    if (!envConfig) {
        setupEnv();
    }

    return envConfig;
}

export default getEnv;