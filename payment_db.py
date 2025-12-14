import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Iterable, Optional

from shared import PaymentLegType, PaymentRequest, PaymentStatus


class PaymentRepository:
    def __init__(self, db_path: str = "payments.db") -> None:
        self.db_path = db_path
        self._init_db()

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS payments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    external_ref TEXT UNIQUE NOT NULL,
                    status TEXT NOT NULL,
                    amount_usd REAL NOT NULL,
                    usdc_amount REAL,
                    local_currency TEXT NOT NULL,
                    local_amount REAL,
                    destination TEXT NOT NULL,
                    fx_rate REAL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS payment_legs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payment_id INTEGER NOT NULL,
                    leg_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    amount REAL,
                    currency TEXT,
                    provider_ref TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(payment_id, leg_type),
                    FOREIGN KEY(payment_id) REFERENCES payments(id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payment_id INTEGER NOT NULL,
                    message TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(payment_id) REFERENCES payments(id)
                )
                """
            )

    def ensure_payment(self, request: PaymentRequest) -> int:
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT id FROM payments WHERE external_ref = ?", (request.external_ref,)
            ).fetchone()
            if existing:
                return int(existing["id"])
            now = self._now()
            cursor = conn.execute(
                """
                INSERT INTO payments(
                    external_ref, status, amount_usd, local_currency, destination, fx_rate,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request.external_ref,
                    PaymentStatus.PENDING.value,
                    request.amount_usd,
                    request.local_currency,
                    request.destination,
                    request.fx_rate,
                    now,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def update_payment_status(
        self, payment_id: int, status: PaymentStatus, usdc_amount: Optional[float] = None,
        local_amount: Optional[float] = None
    ) -> None:
        with self._connect() as conn:
            now = self._now()
            conn.execute(
                """
                UPDATE payments
                SET status = ?, usdc_amount = COALESCE(?, usdc_amount),
                    local_amount = COALESCE(?, local_amount), updated_at = ?
                WHERE id = ?
                """,
                (status.value, usdc_amount, local_amount, now, payment_id),
            )

    def record_leg(
        self,
        payment_id: int,
        leg_type: PaymentLegType,
        status: PaymentStatus,
        amount: Optional[float] = None,
        currency: Optional[str] = None,
        provider_ref: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        with self._connect() as conn:
            now = self._now()
            conn.execute(
                """
                INSERT INTO payment_legs(payment_id, leg_type, status, amount, currency, provider_ref, error, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(payment_id, leg_type)
                DO UPDATE SET status=excluded.status, amount=excluded.amount, currency=excluded.currency,
                    provider_ref=excluded.provider_ref, error=excluded.error, updated_at=excluded.updated_at
                """,
                (
                    payment_id,
                    leg_type.value,
                    status.value,
                    amount,
                    currency,
                    provider_ref,
                    error,
                    now,
                    now,
                ),
            )

    def record_event(self, payment_id: int, message: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO events(payment_id, message, created_at) VALUES (?, ?, ?)",
                (payment_id, message, self._now()),
            )

    def payment_snapshot(self, payment_id: int) -> Dict[str, Any]:
        with self._connect() as conn:
            payment = conn.execute(
                "SELECT * FROM payments WHERE id = ?", (payment_id,)
            ).fetchone()
            legs = conn.execute(
                "SELECT * FROM payment_legs WHERE payment_id = ? ORDER BY id", (payment_id,)
            ).fetchall()
            events = conn.execute(
                "SELECT * FROM events WHERE payment_id = ? ORDER BY id", (payment_id,)
            ).fetchall()
        return {
            "payment": dict(payment) if payment else None,
            "legs": [dict(r) for r in legs],
            "events": [dict(r) for r in events],
        }

    def list_events(self, payment_id: int) -> Iterable[str]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT message FROM events WHERE payment_id = ? ORDER BY id", (payment_id,)
            ).fetchall()
        return [row["message"] for row in rows]

    def _now(self) -> str:
        return datetime.utcnow().isoformat()


__all__ = ["PaymentRepository"]
