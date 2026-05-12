# Iceberg Zero-ETL & Spark Data Pipeline

## ?? 2026 Architecture Modernization
This project represents a state-of-the-art data engineering platform, migrating from traditional Delta Lake / heavy ETL processes to **Apache Iceberg** and **Zero-ETL** architectures. 

### Key Features
1. **Apache Iceberg Catalog:** Universal open table format enabling multiple compute engines (Spark, Snowflake, Athena) to query underlying Parquet files without data movement.
2. **AI-Driven Unstructured Processing (DSPy + Gemma):** An embedded LLM agent sits inside the PySpark pipeline. It dynamically parses, cleans, and structures raw, messy text logs into strict JSON schemas on the fly using DSPy and local Ollama (Gemma) models.
3. **Scalable Batch & Streaming:** Handles complex transformations with data integrity and production-scale performance.

## ??? Tech Stack
*   **Engine:** Apache Spark (PySpark)
*   **Storage Format:** Apache Iceberg
*   **AI Integration:** DSPy, Ollama (Gemma)
*   **Language:** Python
