from dagster import job
from kafka_dagster_pipeline.ops import produce_one_row_from_csv , consume_data_to_clickhouse

@job
def csv_to_kafka_job():
    produce_one_row_from_csv()

@job
def kafka_to_clickhouse_job():
    consume_data_to_clickhouse()
