"""Register the Debezium MySQL connector and wait until it is streaming.

The connector performs the initial snapshot itself, then streams — the same
"one ingestion mechanism" property the sikt project gets from a replication
slot: there is no separate initial-load tool to drift out of sync.
"""

from __future__ import annotations

import argparse
import json
import time

import requests

from apbd_stream.config import SETTINGS, Settings


def connector_config(settings: Settings = SETTINGS) -> dict:
    """The full connector spec — pure, so a test can assert the safety bits."""
    return {
        "name": settings.connector_name,
        "config": {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "tasks.max": "1",
            "database.hostname": settings.connect_mysql_host,
            "database.port": "3306",
            "database.user": "debezium",
            "database.password": "debezium-2026",
            "database.server.id": "184054",
            "topic.prefix": settings.topic_prefix,
            "table.include.list": f"{settings.mysql_db}.bku,{settings.mysql_db}.pagu_anggaran",
            # Snapshot then stream: existing rows arrive as records too, so the
            # aggregate starts from history rather than from zero.
            "snapshot.mode": "initial",
            "tombstones.on.delete": "false",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter.schemas.enable": "false",
            # Without schemas, JsonConverter would base64-encode DECIMAL (and
            # drop its scale — money becomes uninterpretable). Strings survive.
            "decimal.handling.mode": "string",
            "date.handling.mode": "iso_string",
            # Debezium keeps the source's DDL history in a Kafka topic; the
            # single-broker compose needs replication factor 1, and the task
            # runs inside the Docker network so it must use its listener.
            "schema.history.internal.kafka.bootstrap.servers": settings.connect_kafka_bootstrap,
            "schema.history.internal.kafka.topic": f"{settings.connector_name}-schema-history",
            "schema.history.internal.kafka.replication.factor": "1",
        },
    }


def register(settings: Settings = SETTINGS) -> dict:
    spec = connector_config(settings)
    response = requests.post(
        f"{settings.connect_url}/connectors", json=spec, timeout=15
    )
    if response.status_code in (200, 201):
        return response.json()
    if response.status_code == 409:  # already registered: PUT the config so a
        # drifted or previously-failed connector converges on the current spec.
        updated = requests.put(
            f"{settings.connect_url}/connectors/{settings.connector_name}/config",
            json=spec["config"],
            timeout=15,
        )
        if updated.status_code in (200, 201):
            return updated.json()
        raise RuntimeError(
            f"connector update failed: {updated.status_code} {updated.text[:300]}"
        )
    raise RuntimeError(
        f"connector registration failed: {response.status_code} {response.text[:300]}"
    )


def status(settings: Settings = SETTINGS) -> dict:
    response = requests.get(
        f"{settings.connect_url}/connectors/{settings.connector_name}/status", timeout=15
    )
    response.raise_for_status()
    return response.json()


def wait_running(settings: Settings = SETTINGS, timeout: float = 180.0) -> dict:
    deadline = time.monotonic() + timeout
    last: dict = {}
    while time.monotonic() < deadline:
        try:
            last = status(settings)
        except requests.RequestException:
            time.sleep(2)
            continue
        state = last.get("connector", {}).get("state")
        tasks = {task.get("state") for task in last.get("tasks", [])}
        if state == "RUNNING" and tasks == {"RUNNING"}:
            return last
        if state == "FAILED":
            raise RuntimeError(f"connector failed: {last}")
        time.sleep(2)
    raise TimeoutError(f"connector not running after {timeout}s: {last}")


def ensure(settings: Settings = SETTINGS) -> dict:
    register(settings)
    return wait_running(settings)


def main() -> None:
    """`python -m apbd_stream.connector` registers and waits until streaming."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", nargs="?", choices=("ensure", "status"), default="ensure"
    )
    args = parser.parse_args()

    if args.command == "status":
        print(json.dumps(status(), indent=2))
        return
    state = ensure()
    tasks = [task["state"] for task in state["tasks"]]
    print(f"connector {state['connector']['state']}, tasks: {tasks}")


if __name__ == "__main__":
    main()
