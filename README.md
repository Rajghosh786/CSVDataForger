# Distributed CSV Processor (Kafka + Express + PM2)

A distributed, asynchronous CSV ingestion and processing pipeline built with Node.js, TypeScript, Apache Kafka, Express, and PM2.

## Overview & Architecture

The system decouples HTTP file ingestion from CPU/IO-intensive CSV processing using a producer-consumer pattern over Apache Kafka:

1. **HTTP Ingestion Layer (`server.ts`):**
   - Express REST API running on port `1305` (or configured `PORT`).
   - Receives CSV file uploads via `/api/files`.
   - Initializes a Kafka producer (`initProducer()`) to dispatch processing jobs/chunks into a Kafka topic.
   - Remains responsive to HTTP requests without getting blocked by heavy parsing operations.

2. **Distributed Consumer Layer (`worker.ts`):**
   - Headless worker processes managed by PM2 in cluster mode.
   - Each worker connects independently to the database and initializes a Kafka consumer (`startConsumer()`).
   - All workers join the same Kafka consumer group to process messages concurrently across topic partitions.

3. **Process Supervision (`ecosystem.config.cjs`):**
   - PM2 acts as the process manager.
   - Runs 1 instance of the Express server in `fork` mode.
   - Runs 20 parallel instances of the worker consumer in `cluster` mode using Node's native module loader (`--import tsx`).

---

## Project Structure

```server
├── controllers/
│   ├── fileController.ts   # Handles file upload requests, validates payloads, and delegates jobs to Kafka producer    
├── kafka/
│   ├── producer.ts         # Kafka producer initialization and message dispatch
│   └── consumer.ts         # Kafka consumer logic & message processing
├── middleware/
│   ├── upload.ts           # Multer middleware for handling multipart/form-data and file storage/ filtering
├── routes/
│   └── fileRoutes.ts       # Express routes for file upload handling
├── services/
│   └── emailService.ts     # Handles transactional email notifications (e.g., job completion, failure alerts)
├── db.ts                   # Database connection setup
├── ecosystem.config.cjs    # PM2 cluster configuration
├── server.ts               # HTTP API server entry point
├── worker.ts               # Background consumer worker bootstrap
├── package.json
└── README.md
```

---

## Prerequisites

- **Java Development Kit (JDK)**: Version 11 or 17+ (required to run Apache Kafka and ZooKeeper/KRaft locally if not using Docker)
- **Node.js:** v18.19.0+ or v20.6.0+ (requires native `--import` flag support)
- **Apache Kafka Broker:** Running locally or remotely (e.g., via Docker or Confluent Cloud)
- **MongoDB / SQL Database:** Configured in `db.ts`
- **npm** or **pnpm** / **yarn**

---

## Environment Configuration

Create a `.env` file in the root directory:

```env
PORT=1305
CLIENT_URL=http://localhost:5173
KAFKA_BROKER=localhost:9092
MONGO_URI=mongodb+srv://<username>:<password>@clustercanister.z1spozx.mongodb.net/CSVDataForger?retryWrites=true&w=majority
EMAIL_USER=your_email@gmail.com
EMAIL_PASS=your_16_char_app_password
```
---

## Local Setup & Installation (Non-Docker)

Follow these steps to set up the runtime prerequisites, Apache Kafka on Windows, and the backend application.

## Local Setup & Installation (Non-Docker)

Follow these steps to set up the runtime prerequisites, Apache Kafka on Windows, and the backend application.

### 1. Install Java Development Kit (JDK)
Apache Kafka requires Java to run:
* Download and install the latest LTS release (or JDK 17+) from [Adoptium Eclipse Temurin](https://adoptium.net/temurin/releases/).
* Ensure `JAVA_HOME` is configured in your system environment variables and added to `Path`:
  ```cmd
  java -version

### 2. Install & Configure Apache Kafka (Windows)

   Download the binary release (e.g., Scala 2.13 version) from the Apache Kafka Official Downloads.

   Extract the archive directly to C:\kafka (keep the folder path short to avoid Windows MAX_PATH character limits).

Windows 11 wmic Deprecation Patch

Windows 11 removes or restricts the legacy wmic utility, which causes the default Kafka startup script to fail when detecting architecture.

   1.Open a terminal in C:\kafka and edit the file:
      notepad .\bin\windows\kafka-server-start.bat

   2.Locate the following block:
      IF ["%KAFKA_HEAP_OPTS%"] EQU [""] (
         rem detect OS architecture
         wmic os get osarchitecture | find /i "32-bit" >nul 2>&1
         IF NOT ERRORLEVEL 1 (
               rem 32-bit OS
               set KAFKA_HEAP_OPTS=-Xmx512M -Xms512M
         ) ELSE (
               rem 64-bit OS
               set KAFKA_HEAP_OPTS=-Xmx1G -Xms1G
         )
      )

   3. Replace it with:
      IF ["%KAFKA_HEAP_OPTS%"] EQU [""] (
         set KAFKA_HEAP_OPTS=-Xmx1G -Xms1G
      )

      Save and close the file.

3. Clone Repository & Install Dependencies

   git clone [https://github.com/Rajghosh786/CSVDataForger.git](https://github.com/Rajghosh786/CSVDataForger.git)
   cd CSVDataForger/server
   npm install

   Ensure development dependencies include tsx and pm2:

   npm install -D tsx pm2

---

## Running the Application

### 1. Initialize and Start Apache Kafka (KRaft Mode)

Open a terminal in `C:\kafka`:

1. Generate a unique cluster identifier:
   ```cmd
   .\bin\windows\kafka-storage.bat random-uuid

2. Format the log directory using the generated UUID:
   .\bin\windows\kafka-storage.bat format --standalone -t <YOUR_GENERATED_UUID> -c .\config\server.properties

3. Start the Kafka broker:
   .\bin\windows\kafka-server-start.bat .\config\server.properties
   (Keep this terminal open and running.)

### 2. Configure Topic Partitions
   Important Kafka Partition Notice:
   Kafka assigns each partition within a topic to at most one consumer in a consumer group. To utilize all 20 consumer workers simultaneously, your target topic must have at least 20 partitions.

   1. Open a second terminal in C:\kafka: 
      .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --alter --topic csv-processing --partitions 20
   2. Verify the partition allocation:
      .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic csv-processing

### 3. Launch Server & Worker Cluster
   npm run dev

### Development (Unified Foreground Mode)
Runs the server and all 20 consumer workers in a single terminal with live log output:

```bash
npm run dev
```

Pressing `Ctrl + C` gracefully terminates the server and all worker instances.

### Background Mode (Daemonized)
To run all processes detached in the background:

```bash
npm run dev:bg
```

### Stop All Processes
Stops and deregisters all running PM2 instances:

```bash
npm run stop
```

---

## Process Monitoring & Inspection

| Command | Description |
| :--- | :--- |
| `npx pm2 status` | View process list, status (online/stopped), CPU, and memory usage |
| `npx pm2 logs` | View combined live logs from the server and all 20 workers |
| `npx pm2 logs csv-worker` | Stream logs specifically from the 20 consumer instances |
| `npx pm2 logs server` | Stream logs specifically from the Express API server |
| `npx pm2 monit` | Terminal dashboard displaying real-time process metrics |

---

## API Endpoints

### File Upload
- **URL:** `/api/files`
- **Method:** `POST`
- **Body:** `multipart/form-data` containing the CSV payload.
- **Workflow:**
  1. The server receives the file and splits or dispatches records into Kafka.
  2. Workers pull tasks in parallel, update the database, and acknowledge processed offsets.