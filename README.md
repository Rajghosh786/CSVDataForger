# Distributed CSV Processor (Kafka + Express + PM2)

A distributed, asynchronous CSV ingestion and processing pipeline built with **Node.js, TypeScript, Apache Kafka, Express.js, MongoDB, and PM2**.

The system separates HTTP file uploads from CPU/IO-intensive CSV processing by using a **Kafka producer-consumer architecture**. Multiple worker processes consume Kafka messages concurrently and process CSV data in parallel.

---

## Overview & Architecture

The system follows a producer-consumer architecture:

### 1. HTTP Ingestion Layer (`server.ts`)

* Express REST API running on port `1305` or the configured `PORT`.
* Receives CSV file uploads through `/api/files`.
* Uses the Kafka producer to dispatch CSV processing jobs/chunks.
* Keeps the HTTP server responsive while background workers handle processing.

### 2. Kafka Producer (`producer.ts`)

* Initializes the Kafka producer.
* Publishes CSV processing jobs/chunks to the `csv-processing` Kafka topic.
* Kafka distributes messages across topic partitions.

### 3. Distributed Consumer Layer (`worker.ts`)

* Background worker processes are managed by PM2.
* Each worker independently connects to Kafka and the database.
* All workers belong to the same Kafka consumer group.
* Kafka assigns partitions across the available consumers.
* Multiple workers can therefore process different partitions concurrently.

> **Important:** Kafka is responsible for distributing messages across partitions and consumers. PM2 is responsible for managing and running multiple Node.js worker processes.

### 4. Process Supervision (`ecosystem.config.cjs`)

PM2 is used as the process manager.

* Runs **1 Express server instance** in `fork` mode.
* Runs **20 worker instances** in `cluster` mode.
* Workers are started using Node's native module loader with `--import tsx`.

### Architecture

```text
                    ┌──────────────────────┐
                    │       Client         │
                    │    CSV File Upload   │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │    Express Server    │
                    │      server.ts       │
                    │        :1305         │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │    Kafka Producer    │
                    │     producer.ts      │
                    └──────────┬───────────┘
                               │
                               ▼
             ┌─────────────────────────────────┐
             │       Kafka: csv-processing     │
             │          20 partitions          │
             └───────┬───────┬───────┬─────────┘
                     │       │       │
              ┌──────▼──┐ ┌──▼─────┐ ┌▼────────┐
              │ Worker 1│ │Worker 2│ │ Worker 3│
              └─────────┘ └────────┘ └─────────┘
                     │       │       │
                     │       │       │
                     └───────┼───────┘
                             ▼
                    ┌──────────────────┐
                    │     MongoDB      │
                    └──────────────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │  Email Service   │
                    └──────────────────┘
```

---

## Project Structure

```text
server/
├── controllers/
│   └── fileController.ts       # Handles file uploads and dispatches processing jobs
│
├── kafka/
│   ├── producer.ts              # Kafka producer initialization and message dispatch
│   └── consumer.ts              # Kafka consumer logic and message processing
│
├── middleware/
│   └── upload.ts                # Multer middleware for multipart/form-data uploads
│
├── routes/
│   └── fileRoutes.ts            # Express routes for file upload handling
│
├── services/
│   └── emailService.ts          # Transactional email notifications
│
├── db.ts                        # Database connection setup
├── ecosystem.config.cjs         # PM2 process configuration
├── server.ts                    # HTTP API server entry point
├── worker.ts                    # Background Kafka consumer entry point
├── package.json
└── README.md
```

---

## Prerequisites

Make sure the following are installed before running the project.

* **Java Development Kit (JDK):** JDK 17+ recommended
* **Node.js:** Version compatible with the project's `tsx` and `--import` setup
* **Apache Kafka:** Running locally or remotely
* **MongoDB:** Local MongoDB instance or MongoDB Atlas
* **npm**, **pnpm**, or **yarn**
* **Git**

For local Kafka development on Windows, Kafka can be run directly without Docker using KRaft mode.

---

## Environment Configuration

Create a `.env` file inside the `server` directory:

```env
PORT=1305

CLIENT_URL=http://localhost:5173

KAFKA_BROKER=localhost:9092

MONGO_URI=mongodb+srv://<username>:<password>@<cluster>/<database>

EMAIL_USER=your_email@gmail.com
EMAIL_PASS=your_16_char_app_password
```

### Environment Variables

| Variable       | Description                          |
| -------------- | ------------------------------------ |
| `PORT`         | Port used by the Express server      |
| `CLIENT_URL`   | Frontend URL allowed by CORS         |
| `KAFKA_BROKER` | Kafka broker address                 |
| `MONGO_URI`    | MongoDB connection string            |
| `EMAIL_USER`   | Email account used for notifications |
| `EMAIL_PASS`   | Email app password                   |

---

# Local Setup & Installation

The following instructions describe running the project locally on Windows without Docker.

## 1. Install Java Development Kit

Apache Kafka requires Java.

Install JDK 17 or newer and configure `JAVA_HOME`.

Verify the installation:

```cmd
java -version
```

You should see the installed Java version.

---

## 2. Install & Configure Apache Kafka

Download and extract Apache Kafka.

For a simple Windows setup, extract Kafka to:

```text
C:\kafka
```

Keeping the installation path short can help avoid Windows path-length issues.

### Windows 11 WMIC Issue

Some Windows 11 installations no longer include the legacy `wmic` utility.

Older Kafka startup scripts may attempt to use WMIC to detect the operating system architecture.

If Kafka startup fails because of WMIC, edit:

```cmd
notepad .\bin\windows\kafka-server-start.bat
```

Locate:

```bat
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
```

Replace it with:

```bat
IF ["%KAFKA_HEAP_OPTS%"] EQU [""] (

    set KAFKA_HEAP_OPTS=-Xmx1G -Xms1G

)
```

Save the file.

---

## 3. Clone the Repository

Clone the repository:

```bash
git clone https://github.com/Rajghosh786/CSVDataForger.git
```

Navigate to the backend:

```bash
cd CSVDataForger/server
```

Install dependencies:

```bash
npm install
```

If `tsx` and PM2 are not already installed:

```bash
npm install -D tsx pm2
```

---

# Running the Application

## 1. Start Apache Kafka in KRaft Mode

Open a terminal in:

```text
C:\kafka
```

### Generate a Cluster ID

```cmd
.\bin\windows\kafka-storage.bat random-uuid
```

Copy the generated UUID.

### Format the Kafka Storage

Replace `<YOUR_GENERATED_UUID>` with the generated UUID:

```cmd
.\bin\windows\kafka-storage.bat format --standalone -t <YOUR_GENERATED_UUID> -c .\config\server.properties
```

### Start the Kafka Broker

```cmd
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

Keep this terminal running.

Kafka should now be available at:

```text
localhost:9092
```

### Kafka State Reset for Local Development

During local development, Kafka persists messages, offsets, and other broker state on disk. In some troubleshooting scenarios, previously published messages may be consumed again after restarting the broker.

If unexpected behavior occurs, such as:

- The same CSV file being processed multiple times
- Previously submitted jobs being consumed again
- An old large-file processing job unexpectedly starting again
- Consumer offsets or topic state becoming inconsistent during development

a complete local Kafka state reset can be performed.

> ⚠️ **Warning:** This is a destructive development-only operation. It permanently removes the local Kafka data, including stored messages, consumer offsets, topics, and broker state.

### 1. Stop the Kafka Broker

Make sure the Kafka broker is completely stopped before deleting its data.

### 2. Delete the Kafka Data Directory

On the default local setup:

```text
C:\kafka\data
```

---

## 2. Start the Application

The project can be started through the configured npm scripts.

### Development Mode

```bash
npm run dev
```

This starts the Express server and the configured PM2 worker processes.

---

## 3. Configure Kafka Topic Partitions

The project uses the Kafka topic:

```text
csv-processing
```

The worker configuration runs **20 consumer instances**.

Kafka assigns each partition to at most one consumer within the same consumer group.

Therefore:

> To allow all 20 workers to actively consume messages at the same time, the topic should have at least 20 partitions.

Create or increase the topic to 20 partitions:

```cmd
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --alter --topic csv-processing --partitions 20
```

Verify the topic:

```cmd
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic csv-processing
```

You should see the topic's partition information.

### Why 20 Partitions?

For example:

```text
20 Workers
      │
      ▼
20 Kafka Partitions
      │
      ├── Worker 1  → Partition 0
      ├── Worker 2  → Partition 1
      ├── Worker 3  → Partition 2
      ├── ...
      └── Worker 20 → Partition 19
```

If the topic has fewer than 20 partitions, some workers will remain idle because Kafka cannot assign the same partition to multiple consumers within the same consumer group.

---

# Background Mode

To run the application in the background:

```bash
npm run dev:bg
```

PM2 will keep the configured processes running independently of the current terminal.

During the first startup, Kafka consumers may take some time to join the consumer group and receive their partition assignments.

You can inspect the PM2 logs if workers appear to be waiting or rejoining the consumer group.

---

# Stopping the Application

Use:

```bash
npm run stop
```

This stops the PM2-managed server and worker processes.

If running the application directly in the foreground, `Ctrl + C` can be used to terminate the running process.

---

# Process Monitoring & Inspection

| Command                   | Description                                              |
| ------------------------- | -------------------------------------------------------- |
| `npx pm2 status`          | View running PM2 processes, status, CPU and memory usage |
| `npx pm2 logs`            | View combined live logs                                  |
| `npx pm2 logs csv-worker` | View logs from the worker processes                      |
| `npx pm2 logs server`     | View logs from the Express server                        |
| `npx pm2 monit`           | Open the PM2 real-time monitoring dashboard              |

### Check PM2 Status

```bash
npx pm2 status
```

Example:

```text
┌────┬────────────┬──────────┬────────┐
│ id │ name       │ mode     │ status │
├────┼────────────┼──────────┼────────┤
│ 0  │ server     │ fork     │ online │
│ 1  │ csv-worker │ cluster  │ online │
│ 2  │ csv-worker │ cluster  │ online │
│ .  │ .          │ .        │ .      │
│ 20 │ csv-worker │ cluster  │ online │
└────┴────────────┴──────────┴────────┘
```

---

# API Endpoints

## File Upload

### Endpoint

```http
POST /api/files
```

### Request

The endpoint accepts a CSV file using:

```text
multipart/form-data
```

The file is received by the Express server through the configured Multer middleware.

### Processing Flow

```text
Client
  │
  │ POST /api/files
  ▼
Express Server
  │
  │ Validate / receive CSV
  ▼
Kafka Producer
  │
  │ Publish processing jobs
  ▼
Kafka
  │
  │ Distribute messages across partitions
  ▼
Consumer Group
  │
  ├── Worker 1
  ├── Worker 2
  ├── Worker 3
  ├── ...
  └── Worker 20
        │
        ▼
     Database
```

Workers process the assigned Kafka messages and acknowledge the corresponding Kafka offsets after successful processing.

---

# Kafka Consumer Group

All worker processes use the same Kafka consumer group.

This allows Kafka to distribute partitions between the workers.

For example, with:

```text
20 partitions
20 workers
```

Kafka can assign approximately:

```text
Worker 1  → Partition 0
Worker 2  → Partition 1
Worker 3  → Partition 2
...
Worker 20 → Partition 19
```

If one worker stops, Kafka can rebalance the consumer group and assign its partitions to other available workers.

---

# Why Kafka?

The application separates file ingestion from CSV processing.

Without a message queue, the HTTP server could become responsible for:

```text
Upload
  ↓
Read CSV
  ↓
Parse CSV
  ↓
Process records
  ↓
Write database
  ↓
Finish HTTP request
```

For large CSV files, this can keep the HTTP request busy for a long time.

With Kafka:

```text
Upload
  ↓
Express
  ↓
Kafka
  ↓
HTTP request can finish
  ↓
Workers process asynchronously
  ↓
Database
```

This provides:

* Asynchronous processing
* Multiple concurrent workers
* Consumer-group based load distribution
* Fault tolerance through Kafka offsets
* Separation between HTTP ingestion and background processing
* Ability to scale workers independently

---

# Why PM2?

PM2 is used to manage the Node.js processes.

It provides:

* Multiple worker processes
* Process monitoring
* Automatic process management
* Centralized logs
* CPU and memory monitoring
* Easy process management through the ecosystem configuration

The actual message distribution is handled by **Kafka**, while PM2 manages the Node.js processes that consume those messages.

---

# Technologies Used

| Technology   | Purpose                       |
| ------------ | ----------------------------- |
| Node.js      | Runtime environment           |
| TypeScript   | Application development       |
| Express.js   | HTTP API                      |
| Apache Kafka | Distributed message streaming |
| KafkaJS      | Kafka integration             |
| MongoDB      | Data storage                  |
| Mongoose     | MongoDB ODM                   |
| Multer       | File upload handling          |
| PM2          | Process management            |
| tsx          | TypeScript execution          |
| Nodemailer   | Email notifications           |

---

# Key Concepts Demonstrated

This project demonstrates practical implementation of:

* Producer-consumer architecture
* Apache Kafka
* Kafka topics and partitions
* Kafka consumer groups
* Asynchronous processing
* Distributed workers
* Process management with PM2
* CSV file ingestion
* Database persistence
* REST API development
* TypeScript backend development
* Background job processing
* Fault recovery through Kafka consumer rebalancing

---

# Future Improvements

Potential improvements for the system include:
* Handle small CSV files first as the initial processing flow
* Generate an Excel output file and send a download link through email
* Support large CSV files in the 1–2 GB range
* Job progress tracking
* Upload/job status API
* Retry and dead-letter queues
* Failed-record tracking
* Kafka message batching
* CSV validation and schema detection
* Redis-based progress caching
* Object storage such as Amazon S3 for large files
* Horizontal worker scaling
* Docker deployment
* Kafka monitoring
* Prometheus/Grafana metrics
* Authentication and authorization
* File-size limits and streaming uploads
