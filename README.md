# Real-Time Market Data Platform

An ETL pipeline that ingests cryptocurrency trades from Coinbase, streams events through Kafka, and stores them in ClickHouse.

Built to explore distrubuted streaming, fault tolerance, data reliability, and performance optimization.

## Architecture
```
Coinbase WebSocket -> Python Producer -> Kafka -> Python Consumer -> ClickHouse -> SQL Analytics.
```

Python Producer and Consumer are set up to run as a system service in my Linux machine.

Kafka and ClickHouse run inside a Docker Compose.

## Tech Stack

- Python: WebSocket ingestion, data processing, backfilling
- Kafka: Event streaming
- ClickHouse: Column-oriented analytical database
- Docker Compose: Containerized infrastructure
- Ubuntu + `systemd`: 24/7 deployment and service management

## Notes
I am still working on this project. So I will come back and finish this README later.