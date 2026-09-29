import { Kafka } from "kafkajs";
import fs from "fs";
import csv from "csv-parser";
import mongoose from "mongoose";
import { generateExcelAndEmail } from "../services/emailService";

const kafka = new Kafka({
  clientId: "csv-processing-worker",
  brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
});

const consumer = kafka.consumer({
  groupId: "csv-processing-worker",
});

export async function startConsumer() {
  await consumer.connect();

  await consumer.subscribe({  
    topic: "csv-processing",
    fromBeginning: true,
  });

  console.log("Kafka consumer started");

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;

      const job = JSON.parse(message.value.toString());
      console.log("Received CSV job:", job.jobId);

      const jobCollection = mongoose.connection.collection(`job_${job.jobId}`);

      let batch: any[] = [];
      let rowCounter = 0;

      // Wrap in a Promise so the message processing waits for MongoDB to fully complete
      try {
      await new Promise<void>((resolve, reject) => {
        let hasColumns = false;
        let hasRows = false;

        const stream = fs.createReadStream(job.filePath).pipe(csv());

        stream.on("headers", (headers: string[]) => {
          console.log("CSV headers:", headers);

          // 1. Empty check
          if (!headers || headers.length === 0) {
            stream.destroy(new Error("CSV has no columns"));
            return;
          }

          // 2. Single corrupted row check
          if (headers.length === 1 && headers[0].length > 300) {
            stream.destroy(new Error("Corrupted or invalid CSV structure"));
            return;
          }

          hasColumns = true;
        });

        stream.on("data", async (data) => {
          hasRows = true;
          rowCounter++;

          batch.push({
            rowNumber: rowCounter,
            ...data,
          });

          if (batch.length === 500) {
            stream.pause(); // 1. Pause file reading

            const toInsert = [...batch];
            batch = [];

            try {
              await jobCollection.insertMany(toInsert); // 2. Wait for MongoDB
              console.log(`Saved batch of 500 rows to job_${job.jobId}`);
              stream.resume(); // 3. Resume reading once DB has saved them
            } catch (err) {
              stream.destroy(err as Error);
            }
          }
        });

        stream.on("end", async () => {
          if (!hasColumns || !hasRows) {
            reject(new Error("Invalid or empty CSV file"));
            return;
          }
          try {
            // Save any remaining leftover rows
            if (batch.length > 0) {
              await jobCollection.insertMany(batch);
              console.log(`Saved final batch of ${batch.length} rows to job_${job.jobId}`);
              batch = [];
            }

            console.log(`All rows inserted into job_${job.jobId}. Total: ${rowCounter}`);
            resolve();
          } catch (err) {
            reject(err);
          }
        });

        stream.on("error", (err) => {
          reject(err);
        });
      });
      
      //saved in MongoDB
      console.log(`Starting Excel export for job_${job.jobId}...`);
      await generateExcelAndEmail(job.jobId, job.userEmail || "your-email@gmail.com");

      } catch (err: any) {
        console.error(`Skipping job ${job.jobId} due to error:`, err.message);
      }
    },
  });
}