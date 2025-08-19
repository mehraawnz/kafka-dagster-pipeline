from dagster import Definitions, repository
from kafka_dagster_pipeline.jobs import csv_to_kafka_job,kafka_to_clickhouse_job
from kafka_dagster_pipeline.sensors import consumer_10s_sensor,producer_10s_sensor

defs = Definitions(
    jobs=[csv_to_kafka_job,kafka_to_clickhouse_job],
    sensors=[consumer_10s_sensor,producer_10s_sensor],
)

@repository
def my_repository():
    return [csv_to_kafka_job,kafka_to_clickhouse_job]


