# Design Decisions — LogiStream-AI

A simulated logistics platform: driver GPS, order and weather events stream through Kafka into a Bronze layer on MinIO; DuckDB builds Silver and Gold; an ML model predicts delivery delays, tracked in MLflow; Airflow schedules the batch steps.

## Stack

Python · Kafka (`kafka-python`) · MinIO · Parquet · DuckDB · pandas · Airflow · MLflow · scikit-learn (Random Forest) · XGBoost · Docker Compose

## Decisions

| Decision | Chosen over | Why | Trade-off |
| --- | --- | --- | --- |
| One Kafka topic per stream, messages keyed by driver or order ID | A single mixed topic | Each stream is consumed and scaled independently, and one entity's events stay in order within a partition. | More topics to manage. |
| Producer `acks="all"` | `acks=1` | A message is acknowledged only once all in-sync replicas have it, so a broker failure doesn't lose it. | Slightly higher send latency. |
| MinIO (S3-compatible) | Local disk | Same API as AWS S3, so the code moves to the cloud unchanged. | One more service. |
| Parquet partitioned by `year/month/day` | CSV or JSON | Columnar compression, and queries skip partitions they don't need. | Not human-readable. |
| DuckDB for Silver and Gold | Spark | The data fits on one machine; DuckDB runs SQL directly on Parquet with no cluster. | Single-node; Spark becomes the right choice at roughly 10M+ records. |
| Deduplication in Silver by `event_id` (`ROW_NUMBER`) | Deduplicating in the consumer | Bronze keeps the raw record; cleaning happens once, downstream. | Duplicates occupy Bronze storage. |
| SCD Type 2 for driver profiles | Overwriting driver rows | Deliveries are analysed against the driver attributes valid at that time. | More rows; queries must respect validity windows. |
| MLflow tracking and model registry | Saving models manually | Every run's parameters, metrics and model version are recorded and comparable. | One more service. |

## Measured (local laptop run)

GPS producer send rate about 890 messages/s (10,243 events in 11.5 s); Bronze ingestion about 342 records/s; 15,706 events across all streams.

## Next steps

- Write each consumer batch to a uniquely named file (e.g. `part-{partition}-{first_offset}.parquet`) so batches in the same day partition append instead of replacing each other, and replays stay idempotent.
- Commit Kafka offsets manually after a successful write (at-least-once), relying on Silver deduplication.
- Use `auto_offset_reset="earliest"` for the ingestion consumer group.
- Benchmark at the configured 100K-delivery scale.
