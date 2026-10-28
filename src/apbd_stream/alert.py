"""The Slack #finmon-alerts payload for SKPDs over 90% pagu.

Dry-run by default (writes docs/generated/rt_alert_payload.json and prints);
posts only when SLACK_WEBHOOK_URL is set, so running the demo can never
message a real channel by accident.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from decimal import Decimal
from pathlib import Path


def _rupiah(value: Decimal) -> str:
    """Short form for the alert: 'Rp 855.60 M' beats 15 digits in a phone push."""
    billions = value / Decimal("1_000_000_000")
    if billions >= 1:
        return f"Rp {billions.quantize(Decimal('0.01'))} M"
    return f"Rp {(value / Decimal('1_000_000')).quantize(Decimal('0.01'))} Jt"


def build_payload(rows: list[dict], threshold: float = 0.90) -> dict:
    """Slack Block Kit message listing every over-threshold SKPD."""
    if not rows:
        text = (
            f"*SIKT APBD — 0 SKPD melewati {threshold:.0%} pagu*\n"
            "_Realisasi dalam batas, tidak ada yang perlu diberi peringatan._"
        )
        return {
            "text": f"Realisasi APBD: 0 SKPD melewati {threshold:.0%} pagu",
            "blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": text}}],
        }

    header = (
        f"*SIKT APBD — {len(rows)} SKPD melewati {threshold:.0%} pagu*\n"
        "_Realisasi vs pagu, dari stream CDC (terverifikasi mesin)._"
    )
    sections = []
    for row in rows:
        sections.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*{row['kode_skpd']}* {row.get('nama_skpd', '')}\n"
                        f"Realisasi {_rupiah(row['realisasi'])} dari pagu {_rupiah(row['pagu'])}"
                        f" — sisa {_rupiah(row['sisa'])} ({row['persen']}%)"
                    ),
                },
            }
        )
    payload = {
        "text": f"Realisasi APBD: {len(rows)} SKPD melewati {threshold:.0%} pagu",
        "blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": header}}, *sections],
    }
    return payload


def emit(payload: dict, output_dir: Path, webhook_url: str | None) -> dict:
    """Persist the payload; post it only when a webhook is configured."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "rt_alert_payload.json"
    path.write_text(json.dumps(payload, indent=2))

    result = {"payload": str(path), "sent": False}
    if webhook_url:
        request = urllib.request.Request(
            webhook_url,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                result["sent"] = response.status == 200
        except (urllib.error.URLError, TimeoutError) as error:
            raise RuntimeError(f"Slack webhook rejected the alert: {error}") from error
    return result
