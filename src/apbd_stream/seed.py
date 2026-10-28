"""Seed a SIMDA-shaped source: 5 SKPD, pagu, treasurers, and a BKU ledger.

Deterministic (fixed RNG seed): Dinas Sosial lands over 90% of its pagu by
design, so the snapshot itself demonstrates the alert, and every other SKPD
has room for the benchmark to push one across the line on purpose.

    python -m apbd_stream.seed --reset
"""

from __future__ import annotations

import argparse
import random
from datetime import date, timedelta
from decimal import Decimal

import pymysql

from apbd_stream.config import SETTINGS, Settings

SKPDS = [
    ("1.01.01", "Sekretariat Daerah", Decimal("450000000000"), Decimal("0.52")),
    ("1.02.02", "Dinas Kesehatan", Decimal("1110000000000"), Decimal("0.57")),
    ("1.03.03", "Dinas Pendidikan", Decimal("900000000000"), Decimal("0.64")),
    ("1.04.04", "Dinas Pekerjaan Umum", Decimal("700000000000"), Decimal("0.41")),
    ("1.05.05", "Dinas Sosial", Decimal("920000000000"), Decimal("0.93")),
]

URAIAN = [
    "Belanja bahan medis puskesmas",
    "Belanja ATK dan cetak dokumen",
    "Honor kegiatan pengadaan",
    "Belanja jasa pendidikan daerah",
    "Pemeliharaan gedung kantor",
    "Belanja operasional kendaraan dinas",
]

DDL = [
    """
    CREATE TABLE IF NOT EXISTS skpd (
        kode_skpd VARCHAR(16) PRIMARY KEY,
        nama_skpd VARCHAR(120) NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pagu_anggaran (
        id INT AUTO_INCREMENT PRIMARY KEY,
        kode_skpd VARCHAR(16) NOT NULL,
        tahun SMALLINT NOT NULL,
        pagu DECIMAL(20,2) NOT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_pagu_skpd (kode_skpd)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bendahara (
        id_bendahara INT AUTO_INCREMENT PRIMARY KEY,
        nama VARCHAR(120) NOT NULL,
        nik VARCHAR(16) NOT NULL,
        rekening VARCHAR(20) NOT NULL,
        kode_skpd VARCHAR(16) NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bku (
        id_bku INT AUTO_INCREMENT PRIMARY KEY,
        kode_skpd VARCHAR(16) NOT NULL,
        jenis ENUM('PENERIMAAN','PENGELUARAN') NOT NULL DEFAULT 'PENGELUARAN',
        uraian VARCHAR(200) NOT NULL,
        jumlah DECIMAL(20,2) NOT NULL,
        nik_bendahara VARCHAR(16) NOT NULL,
        rekening_tujuan VARCHAR(20) NOT NULL,
        tanggal DATE NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        KEY idx_bku_skpd (kode_skpd)
    )
    """,
]


def connect(settings: Settings = SETTINGS, database: str | None = None):
    return pymysql.connect(
        host=settings.mysql_host,
        port=settings.mysql_port,
        user=settings.mysql_user,
        password=settings.mysql_password,
        database=database if database is not None else settings.mysql_db,
        charset="utf8mb4",
        autocommit=True,
    )


def ensure_schema(settings: Settings = SETTINGS) -> None:
    connection = connect(settings)
    with connection.cursor() as cursor:
        for statement in DDL:
            cursor.execute(statement)
    connection.close()


def _split_total(total: Decimal, count: int, rng: random.Random) -> list[Decimal]:
    """`count` positive amounts summing exactly to `total` (rounding absorbed)."""
    weights = [Decimal(str(rng.uniform(0.5, 2.5))) for _ in range(count)]
    weight_sum = sum(weights)
    amounts = [(total * weight / weight_sum).quantize(Decimal("0.01")) for weight in weights]
    amounts[-1] += total - sum(amounts)
    return amounts


def seed(settings: Settings = SETTINGS, reset: bool = False) -> dict:
    ensure_schema(settings)
    connection = connect(settings)
    rng = random.Random(2026)

    with connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM bku")
        existing = cursor.fetchone()[0]
        if existing and not reset:
            connection.close()
            return {"skpd": len(SKPDS), "bku": existing, "note": "already seeded (use --reset)"}
        if reset:
            for table in ("bku", "bendahara", "pagu_anggaran", "skpd"):
                cursor.execute(f"DELETE FROM {table}")

        start = date(2026, 10, 1)
        total_bku = 0
        for index, (kode, nama, pagu, share) in enumerate(SKPDS):
            cursor.execute("INSERT INTO skpd (kode_skpd, nama_skpd) VALUES (%s, %s)", (kode, nama))
            cursor.execute(
                "INSERT INTO pagu_anggaran (kode_skpd, tahun, pagu) VALUES (%s, 2026, %s)",
                (kode, str(pagu)),
            )
            nik = f"3275{index:02d}{rng.randrange(10**10):010d}"[:16]
            rekening = f"0037{rng.randrange(10**12):012d}"
            cursor.execute(
                "INSERT INTO bendahara (nama, nik, rekening, kode_skpd) VALUES (%s, %s, %s, %s)",
                (f"Bendahara {nama}", nik, rekening, kode),
            )

            target = (pagu * share).quantize(Decimal("0.01"))
            amounts = _split_total(target, 60, rng)
            rows = [
                (
                    kode,
                    "PENGELUARAN",
                    URAIAN[rng.randrange(len(URAIAN))],
                    str(amount),
                    nik,
                    rekening,
                    start + timedelta(days=rng.randrange(25)),
                )
                for amount in amounts
            ]
            # A few receipts: the aggregate must count PENGELUARAN only, and
            # the integration test proves it by comparing against this source.
            receipts = [
                (
                    kode,
                    "PENERIMAAN",
                    "Dana transfer dari pusat",
                    str(amount),
                    nik,
                    rekening,
                    start + timedelta(days=rng.randrange(25)),
                )
                for amount in _split_total(target / 10, 2, rng)
            ]
            cursor.executemany(
                """
                INSERT INTO bku (kode_skpd, jenis, uraian, jumlah, nik_bendahara,
                                 rekening_tujuan, tanggal)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                rows + receipts,
            )
            total_bku += len(rows) + len(receipts)

    connection.close()
    return {"skpd": len(SKPDS), "pagu": len(SKPDS), "bku": total_bku}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reset", action="store_true", help="re-seed from scratch")
    args = parser.parse_args()

    result = seed(reset=args.reset)
    print(result)


if __name__ == "__main__":
    main()
