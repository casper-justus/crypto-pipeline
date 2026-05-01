# Changelog

All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](https://semver.org/).

## [1.0.0] - 2026-05-01
### Added
- Real-time crypto data streaming pipeline using Kafka, Python, and PostgreSQL
- Kafka producer ingesting live price data from crypto WebSocket feeds
- Kafka consumer writing processed data to PostgreSQL
- Docker Compose setup for Kafka, Zookeeper, PostgreSQL, and Grafana
- Grafana dashboard for real-time price visualization
- `init.sql` for PostgreSQL schema initialization
- Makefile for common dev tasks
- MIT License
- CONTRIBUTING.md guide
