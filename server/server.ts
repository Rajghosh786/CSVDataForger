import express from 'express'
import cors from 'cors'
import { config } from 'dotenv'
config()
import fileRoutes from "./routes/fileRoutes"
import { initProducer } from './kafka/producer'
import { startConsumer } from './kafka/consumer'
import { connectingDB } from "./db";

const app = express()
const PORT = process.env.PORT || 1305;

app.use(cors({
    origin: process.env.CLIENT_URL,
    methods: ["GET", "POST", "PUT", "DELETE"],
    credentials: true
}));

app.use(express.json());

app.use('/api/files',fileRoutes);

(async function bootstrap() {
    try {
        await initProducer();
        await startConsumer(); 
    } catch (error) {
        console.log(error)
    }
})()

connectingDB()
app.listen(PORT,()=>{
    console.log(`Server started on ${PORT}`)
})
