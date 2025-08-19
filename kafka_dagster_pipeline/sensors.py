from dagster import sensor, RunRequest
from kafka_dagster_pipeline.ops import get_next_index, CSV_PATH
from kafka_dagster_pipeline.jobs import csv_to_kafka_job,kafka_to_clickhouse_job


def count_rows(csv_path: str):
    with open(csv_path, "r", encoding="utf-8") as f:
        return max(sum(1 for _ in f) - 1, 0)
    
def new_messages_available():
    return True



@sensor(job=csv_to_kafka_job, minimum_interval_seconds=10)
def producer_10s_sensor(_context):
    idx = get_next_index()
    if idx >= count_rows(CSV_PATH):
        return
    return RunRequest(run_key=str(idx))

@sensor(job=kafka_to_clickhouse_job, minimum_interval_seconds=10)
def consumer_10s_sensor(_context):
    if new_messages_available():
        yield RunRequest(run_key=None, run_config={})

