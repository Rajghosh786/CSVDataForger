import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "csv-upload-service",
  brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
});

const producer = kafka.producer();

export async function initProducer() {
  await producer.connect();
  console.log("Kafka producer connected");
}

export async function disconnectProducer() {
  await producer.disconnect();
}

export interface CsvJobPayload {
  jobId: string;
  fileName: string;
  filePath: string;
  size: number;
  userEmail:string;
}

export async function sendCsvJob(jobData: CsvJobPayload) {
  await producer.send({
    topic: "csv-processing",
    messages: [
      {
        key: jobData.jobId,
        value: JSON.stringify(jobData),
      },
    ],
  });

  console.log(`Kafka event sent for jobId: ${jobData.jobId}`);
}