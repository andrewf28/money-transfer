# @@@SNIPSTART python-money-transfer-project-template-withdraw
import asyncio
import os
import random
from typing import Optional

from temporalio import activity

from banking_service import BankingService, InvalidAccountError
from payment_db import PaymentRepository
from shared import (
    PaymentDetails,
    PaymentLegType,
    PaymentRequest,
    PaymentStatus,
)


class BankingActivities:
    def __init__(self):
        self.bank = BankingService("bank-api.example.com")

    @activity.defn
    async def withdraw(self, data: PaymentDetails) -> str:
        reference_id = f"{data.reference_id}-withdrawal"
        try:
            confirmation = await asyncio.to_thread(
                self.bank.withdraw, data.source_account, data.amount, reference_id
            )
            return confirmation
        except InvalidAccountError:
            raise
        except Exception:
            activity.logger.exception("Withdrawal failed")
            raise

    # @@@SNIPEND
    # @@@SNIPSTART python-money-transfer-project-template-deposit
    @activity.defn
    async def deposit(self, data: PaymentDetails) -> str:
        reference_id = f"{data.reference_id}-deposit"
        try:
            confirmation = await asyncio.to_thread(
                self.bank.deposit, data.target_account, data.amount, reference_id
            )
            """
            confirmation = await asyncio.to_thread(
                self.bank.deposit_that_fails,
                data.target_account,
                data.amount,
                reference_id,
            )
            """
            return confirmation
        except InvalidAccountError:
            raise
        except Exception:
            activity.logger.exception("Deposit failed")
            raise

    # @@@SNIPEND

    # @@@SNIPSTART python-money-transfer-project-template-refund
    @activity.defn
    async def refund(self, data: PaymentDetails) -> str:
        reference_id = f"{data.reference_id}-refund"
        try:
            confirmation = await asyncio.to_thread(
                self.bank.deposit, data.source_account, data.amount, reference_id
            )
            return confirmation
        except InvalidAccountError:
            raise
        except Exception:
            activity.logger.exception("Refund failed")
            raise

    # @@@SNIPEND


class StablecoinActivities:
    """Activities supporting a staged USD -> USDC -> local currency transfer."""

    def __init__(self, repo: Optional[PaymentRepository] = None) -> None:
        self.repo = repo or PaymentRepository(os.getenv("PAYMENTS_DB", "payments.db"))

    @activity.defn
    async def initialize_payment(self, request: PaymentRequest) -> int:
        payment_id = await asyncio.to_thread(self.repo.ensure_payment, request)
        await asyncio.to_thread(
            self.repo.record_event,
            payment_id,
            f"Initialized payment for {request.amount_usd} USD to {request.destination}",
        )
        return payment_id

    @activity.defn
    async def apply_fx_and_fees(self, payment_id: int, request: PaymentRequest) -> dict:
        usd = request.amount_usd
        usdc_amount = usd  # assuming parity for the demo
        local_amount = request.requested_local_amount or round(usd * request.fx_rate, 2)
        fee = round(local_amount * 0.0025, 2)
        net_local = round(local_amount - fee, 2)
        await asyncio.to_thread(
            self.repo.update_payment_status,
            payment_id,
            PaymentStatus.PENDING,
            usdc_amount,
            net_local,
        )
        await asyncio.to_thread(
            self.repo.record_event,
            payment_id,
            f"Applied FX {request.fx_rate} and fee {fee}; net local amount {net_local}",
        )
        return {
            "usdc_amount": usdc_amount,
            "local_amount": net_local,
            "fee": fee,
        }

    @activity.defn
    async def collect_usd(self, payment_id: int, amount_usd: float) -> str:
        provider_ref = f"collect-{payment_id}-{random.randint(1000,9999)}"
        await asyncio.to_thread(
            self.repo.record_leg,
            payment_id,
            PaymentLegType.COLLECT,
            PaymentStatus.COMPLETED,
            amount_usd,
            "USD",
            provider_ref,
        )
        await asyncio.to_thread(
            self.repo.record_event,
            payment_id,
            f"Collected {amount_usd} USD (ref {provider_ref})",
        )
        return provider_ref

    @activity.defn
    async def mint_usdc(self, payment_id: int, amount_usdc: float) -> str:
        provider_ref = f"mint-{payment_id}-{random.randint(1000,9999)}"
        await asyncio.to_thread(
            self.repo.record_leg,
            payment_id,
            PaymentLegType.MINT,
            PaymentStatus.COMPLETED,
            amount_usdc,
            "USDC",
            provider_ref,
        )
        await asyncio.to_thread(
            self.repo.record_event,
            payment_id,
            f"Minted {amount_usdc} USDC (ref {provider_ref})",
        )
        return provider_ref

    @activity.defn
    async def offramp_local(self, payment_id: int, amount_local: float, currency: str) -> str:
        provider_ref = f"offramp-{payment_id}-{random.randint(1000,9999)}"
        await asyncio.to_thread(
            self.repo.record_leg,
            payment_id,
            PaymentLegType.OFFRAMP,
            PaymentStatus.COMPLETED,
            amount_local,
            currency,
            provider_ref,
        )
        await asyncio.to_thread(
            self.repo.record_event,
            payment_id,
            f"Sent {amount_local} {currency} to destination (ref {provider_ref})",
        )
        return provider_ref

    @activity.defn
    async def flag_for_reconciliation(self, payment_id: int, reason: str) -> None:
        await asyncio.to_thread(
            self.repo.update_payment_status,
            payment_id,
            PaymentStatus.NEEDS_RECONCILIATION,
        )
        await asyncio.to_thread(
            self.repo.record_event,
            payment_id,
            f"Marked for reconciliation: {reason}",
        )

    @activity.defn
    async def mark_payment_complete(self, payment_id: int) -> None:
        await asyncio.to_thread(
            self.repo.update_payment_status,
            payment_id,
            PaymentStatus.COMPLETED,
        )
        await asyncio.to_thread(
            self.repo.record_event,
            payment_id,
            "Payment completed",
        )

    @activity.defn
    async def mark_payment_failed(self, payment_id: int, reason: str) -> None:
        await asyncio.to_thread(
            self.repo.update_payment_status,
            payment_id,
            PaymentStatus.FAILED,
        )
        await asyncio.to_thread(
            self.repo.record_event,
            payment_id,
            f"Payment failed: {reason}",
        )

    @activity.defn
    async def mark_payment_cancelled(self, payment_id: int) -> None:
        await asyncio.to_thread(
            self.repo.update_payment_status,
            payment_id,
            PaymentStatus.CANCELLED,
        )
        await asyncio.to_thread(
            self.repo.record_event,
            payment_id,
            "Payment cancelled via signal",
        )
