# CSV Processing System — Architecture

## 1. Overall System

```text
CSV Processing System
│
├── Frontend
│   │
│   └── React
│       │
│       ├── File Upload UI
│       ├── File Selection
│       ├── Upload Request
│       └── File Download UI
│
│
├── Backend
│   │
│   └── Node.js + Express
│       │
│       ├── Upload API
│       │   └── POST /api/files/upload
│       │
│       ├── Download API
│       │   └── GET /api/files/:filename/download
│       │
│       └── File Storage
│           └── /uploads
│
│
├── Message Broker
│   │
│   └── Apache Kafka
│       │
│       └── CSV Processing Topic
│
│
├── Worker
│   │
│   └── CSV Processing Worker
│       │
│       ├── Kafka Consumer
│       ├── CSV Stream
│       ├── CSV Parser
│       ├── Batch Processing
│       └── Database Operations
│
│
└── Database
    │
    └── MongoDB
        │
        ├── Jobs
        └── CSV Records
```

---

# 2. Development Phases

```text
Project
│
├── Phase 1 — Basic File Handling
│   │
│   ├── React Frontend
│   ├── File Upload
│   ├── Express Backend
│   ├── Local File Storage
│   └── File Download
│
│
├── Phase 2 — Asynchronous Processing
│   │
│   ├── Kafka Producer
│   ├── Kafka Topic
│   ├── Kafka Consumer
│   └── Background Worker
│
│
├── Phase 3 — CSV Processing
│   │
│   ├── CSV Streaming
│   ├── CSV Parsing
│   ├── Chunking
│   └── Batch Processing
│
│
├── Phase 4 — Database
│   │
│   ├── MongoDB
│   ├── Job Collection
│   └── CSV Records Collection
│
│
└── Phase 5 — Production Features
    │
    ├── Job Status
    ├── Progress Tracking
    ├── Error Handling
    ├── Retry Mechanism
    ├── Duplicate Handling
    └── File Cleanup
```

---

# 3. Phase 1 — Basic File Upload

For the first phase, Kafka and MongoDB are NOT involved.

```text
User
│
│ Select CSV
↓
React Frontend
│
│ HTTP POST
│ multipart/form-data
↓
Express API
│
│ File Upload
↓
Local File System
│
└── uploads/
    │
    ├── file1.csv
    ├── file2.csv
    └── file3.csv
```

---

# 4. Phase 1 — File Download

```text
User
│
│ Click Download
↓
React Frontend
│
│ HTTP GET
↓
Express API
│
│ Read File
↓
Local File System
│
└── uploads/file1.csv
        │
        ↓
     Browser
```

---

# 5. Phase 2 — Kafka Integration

Once upload/download works, Kafka is introduced.

```text
User
│
↓
React
│
↓
Express API
│
├── Save CSV
│      ↓
│   /uploads
│
└── Publish Message
       ↓
     Kafka
       │
       └── csv-processing topic
                │
                ↓
          CSV Worker
```

The Kafka message contains **metadata/reference information**, not the entire CSV file.

Example:

```json
{
  "jobId": "123",
  "fileName": "customers.csv",
  "filePath": "uploads/customers.csv"
}
```

---

# 6. Phase 3 — CSV Worker

The worker receives the Kafka message and processes the file.

```text
Kafka
│
│ Consumer
↓
CSV Worker
│
├── Get File Path
│
├── Open File Stream
│
├── CSV Parser
│
├── Read Rows
│
├── Create Batches
│
└── Send Batches
        ↓
     MongoDB
```

---

# 7. Large CSV Processing

The important concept is **streaming**.

We do NOT do:

```text
2 GB CSV
   ↓
Load entire file into RAM
   ↓
Process
```

Instead:

```text
2 GB CSV
│
↓
File Stream
│
├── Chunk
│    ↓
│   Parse
│    ↓
│   Batch
│    ↓
│   MongoDB
│
├── Chunk
│    ↓
│   Parse
│    ↓
│   Batch
│    ↓
│   MongoDB
│
├── Chunk
│    ↓
│   Parse
│    ↓
│   Batch
│    ↓
│   MongoDB
│
└── ...
```

---

# 8. Complete Data Flow

Eventually the entire system will look like this:

```text
                         USER
                           │
                           ↓
                    REACT FRONTEND
                           │
                           │ HTTP
                           ↓
                    EXPRESS SERVER
                           │
              ┌────────────┴────────────┐
              │                         │
              ↓                         ↓
       LOCAL FILE SYSTEM            MONGODB
              │                         │
              │                         │
              │                         │
              ↓                         │
           CSV FILE                     │
              │                         │
              │                         │
              └──────────┐              │
                         │              │
                         ↓              │
                    KAFKA PRODUCER      │
                         │              │
                         ↓              │
                  KAFKA TOPIC           │
                         │              │
                         ↓              │
                  KAFKA CONSUMER        │
                         │              │
                         ↓              │
                    CSV WORKER          │
                         │              │
                         ↓              │
                   FILE STREAM          │
                         │              │
                         ↓              │
                    CSV PARSER          │
                         │              │
                         ↓              │
                   BATCH PROCESSING     │
                         │              │
                         └──────────────┘
                                │
                                ↓
                             MONGODB
```

---

# 9. Responsibilities

Each component has a specific responsibility.

```text
React
│
└── User Interface
    └── Upload / Download / Status


Express
│
└── HTTP API
    └── Receive requests
    └── Save files
    └── Return responses


Local File System
│
└── Store original CSV files


Kafka
│
└── Message Broker
    └── Communicate processing jobs
    └── Decouple API from Worker


CSV Worker
│
└── Background Processing
    └── Consume Kafka messages
    └── Read CSV
    └── Parse CSV
    └── Create batches
    └── Write to MongoDB


MongoDB
│
└── Persistent Data Storage
    ├── Job information
    └── Parsed CSV records
```

---

# 10. Final Target Architecture

```text
                           ┌──────────────┐
                           │     USER     │
                           └──────┬───────┘
                                  │
                                  ↓
                           ┌──────────────┐
                           │    REACT     │
                           │  FRONTEND    │
                           └──────┬───────┘
                                  │
                             HTTP Request
                                  │
                                  ↓
                           ┌──────────────┐
                           │   EXPRESS    │
                           │     API      │
                           └──────┬───────┘
                                  │
                    ┌─────────────┴─────────────┐
                    │                           │
                    ↓                           ↓
             ┌──────────────┐            ┌──────────────┐
             │ LOCAL FILE   │            │   MONGODB    │
             │   STORAGE    │            │              │
             └──────┬───────┘            │ Jobs         │
                    │                    │ Records      │
                    │                    └──────────────┘
                    │
                    ↓
             ┌──────────────┐
             │    KAFKA     │
             │    TOPIC     │
             └──────┬───────┘
                    │
                    ↓
             ┌──────────────┐
             │ CSV WORKER   │
             │              │
             │ Kafka        │
             │ Consumer     │
             └──────┬───────┘
                    │
                    ↓
             ┌──────────────┐
             │ FILE STREAM  │
             └──────┬───────┘
                    │
                    ↓
             ┌──────────────┐
             │ CSV PARSER   │
             └──────┬───────┘
                    │
                    ↓
             ┌──────────────┐
             │   BATCH      │
             │  PROCESSING  │
             └──────┬───────┘
                    │
                    ↓
             ┌──────────────┐
             │   MONGODB    │
             │    RECORDS   │
             └──────────────┘
```

## The core idea

The entire project can be understood as:

```text
UPLOAD
   ↓
STORE
   ↓
QUEUE
   ↓
WORK
   ↓
STREAM
   ↓
PARSE
   ↓
BATCH
   ↓
STORE
```

**Phase 1 is only:**

```text
React
  ↓
Express
  ↓
Local Storage
  ↓
Download
```
