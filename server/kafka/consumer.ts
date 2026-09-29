import { Kafka, PartitionAssigners } from "kafkajs";
import fs from "fs";
import csv from "csv-parser";
import mongoose from "mongoose";
import { generateExcelAndEmail } from "../services/emailService";

const BATCH_SIZE = 500;
const HEARTBEAT_INTERVAL_MS = 3000;

const kafka = new Kafka({
  clientId: "csv-processing-worker",
  brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
});

const consumer = kafka.consumer({
  groupId: "csv-processing-worker",
  sessionTimeout: 45000,
  rebalanceTimeout: 60000,
  heartbeatInterval: HEARTBEAT_INTERVAL_MS,
  // Keep assigned partitions when other workers join/leave so a long CSV
  // job is not yanked mid-process just because the group membership changed.
  // partitionAssigners: [PartitionAssigners.cooperativeSticky],
});

export async function startConsumer() {
  await consumer.connect();

  await consumer.subscribe({
    topic: "csv-processing",
    fromBeginning: true,
  });

  console.log("Kafka consumer started");

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message, heartbeat }) => {
      if (!message.value) return;

      const job = JSON.parse(message.value.toString());
      console.log("Received CSV job:", job.jobId);

      const jobCollection = mongoose.connection.collection(`job_${job.jobId}`);

      let batch: any[] = [];
      let rowCounter = 0;
      let heartbeatTimer: ReturnType<typeof setInterval> | undefined;

      try {
        await heartbeat();

        // If Kafka redelivers this job (crash / revoke before commit), replace
        // prior rows instead of inserting a second copy. This is not exactly-once;
        // it makes at-least-once retries safe for this collection.
        await jobCollection.deleteMany({});

        await new Promise<void>((resolve, reject) => {
          let hasColumns = false;
          let hasRows = false;
          let settled = false;
          let writeChain: Promise<void> = Promise.resolve();

          const settleResolve = () => {
            if (!settled) {
              settled = true;
              resolve();
            }
          };

          const settleReject = (err: unknown) => {
            if (!settled) {
              settled = true;
              reject(err);
            }
          };

          const stream = fs.createReadStream(job.filePath).pipe(csv());

          heartbeatTimer = setInterval(() => {
            heartbeat().catch((err) => {
              stream.destroy(err);
            });
          }, HEARTBEAT_INTERVAL_MS);

          const enqueueWrite = (work: () => Promise<void>) => {
            writeChain = writeChain.then(work).catch((err) => {
              stream.destroy(err);
              throw err;
            });
            return writeChain;
          };

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

          stream.on("data", (data) => {
            hasRows = true;
            rowCounter++;

            batch.push({
              rowNumber: rowCounter,
              ...data,
            });

            if (batch.length === BATCH_SIZE) {
              stream.pause();

              const toInsert = batch;
              batch = [];

              enqueueWrite(async () => {
                await jobCollection.insertMany(toInsert);
                console.log(`Saved batch of ${BATCH_SIZE} rows to job_${job.jobId}`);
                await heartbeat();
                stream.resume();
              });
            }
          });

          stream.on("end", () => {
            enqueueWrite(async () => {
              if (!hasColumns || !hasRows) {
                throw new Error("Invalid or empty CSV file");
              }

              if (batch.length > 0) {
                await jobCollection.insertMany(batch);
                console.log(`Saved final batch of ${batch.length} rows to job_${job.jobId}`);
                batch = [];
              }

              console.log(`All rows inserted into job_${job.jobId}. Total: ${rowCounter}`);
            }).then(settleResolve, settleReject);
          });

          stream.on("error", (err) => {
            settleReject(err);
          });
        });

        const nextOffset = (BigInt(message.offset) + 1n).toString();
        await consumer.commitOffsets([
          { topic, partition, offset: nextOffset },
        ]);
        console.log(`Committed Kafka offset for job ${job.jobId} (partition ${partition}, offset ${nextOffset})`);

        //saved in MongoDB
        // console.log(`Starting Excel export for job_${job.jobId}...`);
        // await generateExcelAndEmail(job.jobId, job.userEmail || "your-email@gmail.com");
      } catch (err: any) {
        console.error(`Job ${job.jobId} failed; offset will not be committed:`, err.message);
        throw err;
      } finally {
        if (heartbeatTimer) {
          clearInterval(heartbeatTimer);
        }
      }
    },
  });
}
