# End-to-End Spark Data Pipeline

This project implements a scalable data platform that handles both batch and streaming transformations using PySpark and Delta Lake. It is designed to process complex datasets through a structured multi-layer architecture to ensure data integrity and performance at scale.

## Architecture Overview
The pipeline follows the medallion architecture pattern to transform raw data into high-quality technical datasets.
- **Bronze Layer**: Captures raw ingestion from sources without modifications to preserve the original state.
- **Silver Layer**: Applies data validation, schema enforcement, and cleansing to provide a reliable source for analytics.
- **Gold Layer**: Aggregates and optimizes data for specific business logic and high-performance reporting.

## Key Features
- Distributed data processing using the Apache Spark engine.
- ACID transactions and time travel capabilities provided by Delta Lake storage.
- Support for unified batch and real-time streaming workflows.
- Automated schema evolution and data quality checks.

## Implementation Details
The core logic is contained in `pipeline.py`. This script manages the Spark session configuration and defines the transformation logic required to move data through the different processing stages.
