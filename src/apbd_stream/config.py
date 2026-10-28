"""Every host, port, and threshold in one place, all overridable by env.

The defaults match compose.yaml: this stack runs beside sikt-mini-lakehouse,
so its MySQL is on 3307 instead of 3306.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


@dataclass(frozen=True)
class Settings:
    mysql_host: str = field(default_factory=lambda: _env("APBD_MYSQL_HOST", "localhost"))
    mysql_port: int = field(default_factory=lambda: int(_env("APBD_MYSQL_PORT", "3307")))
    mysql_user: str = field(default_factory=lambda: _env("APBD_MYSQL_USER", "simda"))
    mysql_password: str = field(default_factory=lambda: _env("APBD_MYSQL_PASSWORD", "simda-2026"))
    mysql_root_password: str = field(
        default_factory=lambda: _env("APBD_MYSQL_ROOT_PASSWORD", "root-2026")
    )
    mysql_db: str = field(default_factory=lambda: _env("APBD_MYSQL_DB", "siktm"))

    kafka_bootstrap: str = field(
        default_factory=lambda: _env("APBD_KAFKA_BOOTSTRAP", "localhost:9092")
    )
    connect_url: str = field(default_factory=lambda: _env("APBD_CONNECT_URL", "http://localhost:8080"))
    connector_name: str = field(default_factory=lambda: _env("APBD_CONNECTOR_NAME", "apbd-simda"))
    # Resolved inside the Docker network, where the connector reads the binlog.
    connect_mysql_host: str = field(
        default_factory=lambda: _env("APBD_CONNECT_MYSQL_HOST", "apbd-mysql")
    )
    topic_prefix: str = field(default_factory=lambda: _env("APBD_TOPIC_PREFIX", "apbd"))
    # Resolved inside the Docker network, for the connector's own Kafka clients.
    connect_kafka_bootstrap: str = field(
        default_factory=lambda: _env("APBD_CONNECT_KAFKA_BOOTSTRAP", "apbd-kafka:29092")
    )

    threshold: float = field(default_factory=lambda: float(_env("APBD_THRESHOLD", "0.90")))
    slack_webhook_url: str | None = field(
        default_factory=lambda: _env("SLACK_WEBHOOK_URL", "") or None
    )
    consumer_group: str = field(default_factory=lambda: _env("APBD_CONSUMER_GROUP", "apbd-rt"))
    state_file: Path = field(
        default_factory=lambda: Path(_env("APBD_STATE_FILE", "data/state.json"))
    )
    output_dir: Path = field(
        default_factory=lambda: Path(_env("APBD_OUTPUT_DIR", "docs/generated"))
    )

    @property
    def bku_topic(self) -> str:
        return f"{self.topic_prefix}.{self.mysql_db}.bku"

    @property
    def pagu_topic(self) -> str:
        return f"{self.topic_prefix}.{self.mysql_db}.pagu_anggaran"


SETTINGS = Settings()
