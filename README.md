# Weather Data Streaming Pipeline 

A distributed, event-driven pipeline designed to ingest and store weather telemetry with "exactly-once" delivery guarantees. The system orchestrates data flow from **MySQL** through **Kafka** partitions into **HDFS** for long-term analytical storage.



## Technical Features
* **Event Streaming**: Implemented a **Kafka Producer** to stream weather records from MySQL using **gRPC/Protobuf** serialization.
* **Distributed Storage**: Developed a **Kafka Consumer** to batch and persist stream data into **HDFS** as optimized **Parquet** files.
* **Fault Tolerance**: Engineered custom checkpointing using manual offset management to achieve **exactly-once semantics** during consumer recovery.
* **Data Integrity**: Utilized **atomic HDFS writes** (write-and-move strategy) to prevent file corruption during processing batches.

## Tech Stack
* **Broker**: Apache Kafka
* **Distributed FS**: Hadoop HDFS
* **Database**: MySQL
* **Formats**: Parquet, Protobuf, JSON
* **Orchestration**: Docker & Docker Compose

## Project Structure
* `producer.py`: MySQL-to-Kafka ingestion with gRPC encoding.
* `consumer.py`: Kafka-to-HDFS storage with manual partition assignment and atomic writes.
* `debug.py`: Consumer group monitoring and stream verification.