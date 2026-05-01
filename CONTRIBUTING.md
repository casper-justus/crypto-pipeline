# Contributing

Thank you for your interest in contributing! Contributions are welcome in the form of bug reports, feature requests, pipeline improvements, and documentation fixes.

## Getting Started

1. **Fork** the repository and create a new branch from `main`:
   ```bash
   git checkout -b feat/your-feature-name
   ```
2. Make your changes and test locally using Docker Compose.
3. Open a **Pull Request** against `main` with a clear description of what changed and why.

## What You Can Contribute

- 🔄 **Pipeline improvements** — better Kafka configs, consumer optimizations, schema changes
- 🐛 **Bug fixes** — fixes for data ingestion, PostgreSQL writes, or Docker issues
- 📊 **New data sources** — additional crypto exchanges or WebSocket endpoints
- 📚 **Documentation** — clearer setup guides, architecture diagrams, usage examples

## Pull Request Guidelines

- Keep PRs focused — one change per PR
- Use clear commit messages (e.g. `fix: handle Kafka connection timeout`)
- Ensure Docker Compose still starts cleanly before submitting

## Reporting Issues

When reporting a bug, please include:
- Steps to reproduce
- Relevant logs from `docker compose logs`
- Your OS and Docker version

## Code of Conduct

Be respectful and constructive. Everyone is welcome regardless of experience level.
