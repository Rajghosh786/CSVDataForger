import express from 'express'
import cors from 'cors'
import { config } from 'dotenv'
config()
import fileRoutes from "./routes/fileRoutes"
import { initProducer } from './kafka/producer'
// import { startConsumer } from './kafka/consumer'
import { connectingDB } from "./db";
import fs from "fs";
import path from 'path'
import cron from 'node-cron'

const app = express()
const PORT = process.env.PORT || 1305;

app.use(cors({
    origin: process.env.CLIENT_URL,
    methods: ["GET", "POST", "PUT", "DELETE"],
    credentials: true
}));

app.use(express.json());

app.use('/api/files',fileRoutes);

// Hourly cleanup for physical files older than 24h
// cron.schedule("0 * * * *", () => {
cron.schedule("0 * * * *", () => {
  const exportsDir = path.resolve("./uploads/exports");
  const maxAgeMs = 24 * 60 * 60 * 1000; // 24 hours
  // const maxAgeMs = 3 * 60 * 1000; // 1 minutes
  const now = Date.now();

  if (!fs.existsSync(exportsDir)) return;

  fs.readdir(exportsDir, (err, files) => {
    if (err) return console.error("Error reading exports directory:", err);

    files.forEach((file) => {
      const filePath = path.join(exportsDir, file);
      fs.stat(filePath, (statErr, stats) => {
        if (statErr) return;

        if (now - stats.mtimeMs > maxAgeMs) {
          fs.unlink(filePath, (unlinkErr) => {
            if (!unlinkErr) {
              console.log(`[CLEANUP] Deleted expired file: ${file}`);
            }
          });
        }
      });
    });
  });
});

(async function bootstrap() {
    try {
        await initProducer();
        // await startConsumer(); 
    } catch (error) {
        console.log(error)
    }
})()

connectingDB()
app.listen(PORT,()=>{
    console.log(`Server started on ${PORT}`)
})
