"""ParaEMT Kafka ShareConsumer worker."""

import json
import logging
import os
import shutil
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

import boto3
from confluent_kafka import AcknowledgeType, KafkaError, Producer, ShareConsumer
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    ResourceType,
)

from plugins import Context, InvalidRequest, ParaEmtPlugin
from plugins.base import Result

STATUS_SCHEMA = {
    "type": "struct", "name": "simulation-status.Value", "fields": [
        {"type": "string", "optional": False, "field": "id"},
        {"type": "int64", "name": "org.apache.kafka.connect.data.Timestamp", "version": 1,
         "optional": False, "field": "fecha"},
        {"type": "string", "optional": False, "field": "status"},
        {"type": "string", "optional": True, "field": "started_at"},
        {"type": "string", "optional": True, "field": "finished_at"},
        {"type": "float", "optional": True, "field": "duration_s"},
        {"type": "string", "optional": True, "field": "error"},
        {"type": "string", "optional": True, "field": "metrics"},
    ],
}
RESULT_SCHEMA = {
    "type": "struct", "name": "simulation-result.Value", "fields": [
        {"type": "string", "optional": False, "field": "simulation_id"},
        {"type": "int64", "name": "org.apache.kafka.connect.data.Timestamp", "version": 1,
         "optional": False, "field": "fecha"},
        {"type": "double", "optional": False, "field": "t"},
        {"type": "string", "optional": False, "field": "measurement"},
        {"type": "float", "optional": False, "field": "value"},
    ],
}

running = True
plugin = ParaEmtPlugin()
shared_cache: dict[Any, Any] = {}


def setting(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def stop(signum: int, _frame: Any) -> None:
    global running
    logging.info("Received signal %s; shutting down after the current message", signum)
    running = False


def publish(producer: Producer, topic: str, record: dict[str, Any]) -> None:
    body = json.dumps(record, default=str)
    while True:
        try:
            producer.produce(topic=topic, value=body)
            break
        except BufferError:
            producer.poll(0.1)
    producer.poll(0)


def publish_status(producer: Producer, topic: str, request_id: str, status: str,
                   started: float, finished: float | None = None, error: str | None = None,
                   metrics: dict[str, Any] | None = None) -> None:
    now = datetime.now(timezone.utc)
    payload = {
        "id": request_id,
        "fecha": int(now.timestamp() * 1000),
        "status": status,
        "started_at": datetime.fromtimestamp(started, timezone.utc).isoformat() if status != "queued" else None,
        "finished_at": datetime.fromtimestamp(finished, timezone.utc).isoformat() if finished else None,
        "duration_s": finished - started if finished else None,
        "error": error,
        "metrics": json.dumps(metrics, default=str) if metrics else None,
    }
    publish(producer, topic, {"schema": STATUS_SCHEMA, "payload": payload})
    pending = producer.flush(float(os.getenv("PRODUCER_FLUSH_TIMEOUT", "60")))
    if pending:
        raise RuntimeError(f"{pending} status messages remain undelivered")


def publish_results(producer: Producer, topic: str, request_id: str,
                    results: list[Result]) -> None:
    for result in results:
        payload = {
            "simulation_id": request_id,
            "fecha": int(datetime.now(timezone.utc).timestamp() * 1000),
            "t": float(result.t),
            "measurement": result.measurement,
            "value": float(result.value),
        }
        publish(producer, topic, {"schema": RESULT_SCHEMA, "payload": payload})
    pending = producer.flush(float(os.getenv("PRODUCER_FLUSH_TIMEOUT", "60")))
    if pending:
        raise RuntimeError(f"{pending} result messages remain undelivered")


def process(value: Any, producer: Producer, s3: Any, results_topic: str,
            status_topic: str) -> None:
    if not isinstance(value, dict) or not value.get("id"):
        raise InvalidRequest("Request payload must be an object with an id")
    if value.get("plugin", "paraemt") != "paraemt":
        raise InvalidRequest("This worker accepts only plugin='paraemt'")

    request_id = str(value["id"])
    started = time.time()
    ctx = Context(s3=s3, producer=producer)
    try:
        publish_status(producer, status_topic, request_id, "running", started)
        shared_key = plugin.shared_key(value)
        key = (plugin.name, shared_key)
        shared = shared_cache.get(key)
        if shared is None or (isinstance(shared, str) and not os.path.isdir(shared)):
            shared = plugin.prepare_shared(value, ctx)
            shared_cache[key] = shared
        case_dir = plugin.workdir(value)
        info = plugin.run_case(value, ctx, shared, case_dir)
        results = plugin.extract_results(value, ctx, case_dir)
        publish_results(producer, results_topic, request_id, results)
        finished = time.time()
        publish_status(producer, status_topic, request_id, "completed", started, finished,
                       metrics={"solver": plugin.solver_info(ctx), "details": info.details,
                                "solver_duration_s": info.duration, "result_count": len(results)})
        logging.info("Simulation %s completed in %.3f s", request_id, finished - started)
    except Exception as exc:
        finished = time.time()
        logging.exception("Simulation %s failed", request_id)
        try:
            if "case_dir" in locals() and os.path.isdir(case_dir):
                plugin.failure_artifacts(value, ctx, case_dir)
            publish_status(producer, status_topic, request_id, "failed", started, finished,
                           error=str(exc))
        except Exception:
            logging.exception("Could not publish failure status for %s", request_id)
    finally:
        if "case_dir" in locals():
            shutil.rmtree(case_dir, ignore_errors=True)


def configure_share_group(bootstrap: str, group_id: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    resource = ConfigResource(
        ResourceType.GROUP,
        group_id,
        incremental_configs=[
            ConfigEntry(
                "share.auto.offset.reset", "earliest",
                incremental_operation=AlterConfigOpType.SET,
            )
        ],
    )
    try:
        futures = admin.incremental_alter_configs([resource])
        for future in futures.values():
            future.result(timeout=30)
    except Exception:
        logging.exception("Could not set earliest offset reset for share group %s", group_id)


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s [%(levelname)s] %(message)s")
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    bootstrap = setting("KAFKA_BOOTSTRAP_SERVERS")
    group_id = setting("CONSUMER_GROUP_ID", "paraemt-workers")
    request_topic = setting("REQUEST_TOPIC")
    results_topic = setting("RESULTS_TOPIC")
    status_topic = setting("SIMULATION_STATUS_TOPIC")
    configure_share_group(bootstrap, group_id)
    consumer = ShareConsumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "max.poll.records": 1,
        "max.poll.interval.ms": int(os.getenv("MAX_POLL_INTERVAL_MS", "3600000")),
        "share.acknowledgement.mode": "explicit",
    })
    consumer.subscribe([request_topic])
    producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 100,
                         "compression.type": "lz4"})
    s3 = boto3.client(
        "s3", endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("S3_SECRET_ACCESS_KEY"),
    )
    try:
        while running:
            messages = consumer.poll(1.0)
            for message in messages:
                if message.error():
                    if message.error().code() != KafkaError._PARTITION_EOF:
                        logging.error("Kafka consumer error: %s", message.error())
                    continue
                try:
                    envelope = json.loads(message.value())
                    process(envelope.get("payload"), producer, s3, results_topic, status_topic)
                except (InvalidRequest, json.JSONDecodeError, KeyError) as exc:
                    logging.error("Discarding invalid request: %s", exc)
                    consumer.acknowledge(message, AcknowledgeType.ACCEPT)
                except Exception:
                    logging.exception("Request processing must be retried")
                    consumer.acknowledge(message, AcknowledgeType.RELEASE)
                else:
                    consumer.acknowledge(message, AcknowledgeType.ACCEPT)
                for partition, error in consumer.commit_sync(timeout=10.0).items():
                    if error:
                        logging.error("Acknowledgement failed for %s: %s", partition, error)
    finally:
        consumer.close()
        producer.flush(10)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("Worker startup failed")
        sys.exit(1)
