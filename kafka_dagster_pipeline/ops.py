import csv
import json
import os
from dagster import op
from kafka import KafkaProducer,KafkaConsumer
import clickhouse_connect

STATE_FILE = ".state/offset.txt"
CSV_PATH = "data/orders.csv"
BOOTSTRAP = "172.27.10.243:9092"
TOPIC = "orders"
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_DB = 'default'
CLICKHOUSE_TABLE = 'orders'


def get_next_index():
    os.makedirs(".state", exist_ok=True)
    try:
        return int(open(STATE_FILE).read().strip())
    except:
        return 0


def advance_index(i):
    os.makedirs(".state", exist_ok=True)
    open(STATE_FILE, "w").write(str(i))


@op
def produce_one_row_from_csv(context):
    idx = get_next_index()
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if idx >= len(rows):
        context.log.info("The End of CSV")
        return

    row = rows[idx]

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    producer.send(TOPIC, value=row)
    producer.flush()
    producer.close()

    context.log.info(f"Sent row #{idx} to Kafka")
    advance_index(idx + 1)

@op
def consume_data_to_clickhouse(context):
    client = clickhouse_connect.get_client(
        host='localhost',
        user='default',
        password='123456',
        database='default'
    )
    
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='dagster_group'
    )
    
    context.log.info(f"Listening to Kafka topic: {TOPIC}")

    messages = consumer.poll(timeout_ms=5000)

    if not messages:
        context.log.info("No new messages found.")
        return

    for tp, batch in messages.items():
        for message in batch:
            try:
                raw_value = message.value.decode("utf-8").strip()
                if not raw_value:
                    continue
                
                # تبدیل به dict
                data = json.loads(raw_value)

                row = (
                    int(data.get("order_id")),
                    int(data.get("user_id")),
                    data.get("eval_set"),
                    int(data.get("order_number")),
                    int(data.get("order_dow")),
                    int(data.get("order_hour_of_day")),
                    float(data.get("days_since_prior_order")) if data.get("days_since_prior_order") else None
                )

                # Insert در ClickHouse
                client.insert(
                    CLICKHOUSE_TABLE,
                    [row],
                    column_names=[
                        "order_id", "user_id", "eval_set",
                        "order_number", "order_dow",
                        "order_hour_of_day", "days_since_prior_order"
                    ]
                )
                context.log.info(f"Inserted row: {row}")

            except Exception as e:
                context.log.error(f"Error processing message: {e}")

    consumer.close()
    context.log.info("Finished consuming messages ✅")

