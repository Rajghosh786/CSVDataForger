import { config } from "dotenv";
config();

import { connectingDB } from "./db";
import { startConsumer } from "./kafka/consumer";

(async function bootstrap() {
    try {
        await connectingDB();
        await startConsumer();
    } catch (error) {
        console.error("Worker failed to start:", error);
    }
})();