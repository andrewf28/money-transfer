# @@@SNIPSTART python-money-transfer-project-template-workflows
from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from activities import BankingActivities, StablecoinActivities
    from shared import (
        MONEY_TRANSFER_TASK_QUEUE_NAME,
        PaymentDetails,
        PaymentLegType,
        PaymentRequest,
        PaymentStatus,
        STABLECOIN_TASK_QUEUE_NAME,
    )


@workflow.defn
class MoneyTransfer:
    @workflow.run
    async def run(self, payment_details: PaymentDetails) -> str:
        retry_policy = RetryPolicy(
            maximum_attempts=3,
            maximum_interval=timedelta(seconds=2),
            non_retryable_error_types=["InvalidAccountError", "InsufficientFundsError"],
        )

        # Withdraw money
        withdraw_output = await workflow.execute_activity_method(
            BankingActivities.withdraw,
            payment_details,
            start_to_close_timeout=timedelta(seconds=5),
            retry_policy=retry_policy,
        )

        # Deposit money
        try:
            deposit_output = await workflow.execute_activity_method(
                BankingActivities.deposit,
                payment_details,
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=retry_policy,
            )

            result = f"Transfer complete (transaction IDs: {withdraw_output}, {deposit_output})"
            return result
        except ActivityError as deposit_err:
            # Handle deposit error
            workflow.logger.error(f"Deposit failed: {deposit_err}")
            # Attempt to refund
            try:
                refund_output = await workflow.execute_activity_method(
                    BankingActivities.refund,
                    payment_details,
                    start_to_close_timeout=timedelta(seconds=5),
                    retry_policy=retry_policy,
                )
                workflow.logger.info(
                    f"Refund successful. Confirmation ID: {refund_output}"
                )
                raise deposit_err
            except ActivityError as refund_error:
                workflow.logger.error(f"Refund failed: {refund_error}")
                raise refund_error


@workflow.defn
class CollectUSDWorkflow:
    def __init__(self) -> None:
        self.status = PaymentStatus.PENDING

    @workflow.query
    def get_status(self) -> str:
        return self.status.value

    @workflow.run
    async def run(self, payment_id: int, amount_usd: float) -> dict:
        ref = await workflow.execute_activity_method(
            StablecoinActivities.collect_usd,
            payment_id,
            amount_usd,
            start_to_close_timeout=timedelta(seconds=10),
        )
        self.status = PaymentStatus.COMPLETED
        return {"provider_ref": ref, "amount": amount_usd}


@workflow.defn
class MintUSDCWorkflow:
    def __init__(self) -> None:
        self.status = PaymentStatus.PENDING

    @workflow.query
    def get_status(self) -> str:
        return self.status.value

    @workflow.run
    async def run(self, payment_id: int, amount_usdc: float) -> dict:
        ref = await workflow.execute_activity_method(
            StablecoinActivities.mint_usdc,
            payment_id,
            amount_usdc,
            start_to_close_timeout=timedelta(seconds=10),
        )
        self.status = PaymentStatus.COMPLETED
        return {"provider_ref": ref, "amount": amount_usdc}


@workflow.defn
class OfframpWorkflow:
    def __init__(self) -> None:
        self.status = PaymentStatus.PENDING

    @workflow.query
    def get_status(self) -> str:
        return self.status.value

    @workflow.run
    async def run(self, payment_id: int, amount_local: float, currency: str) -> dict:
        ref = await workflow.execute_activity_method(
            StablecoinActivities.offramp_local,
            payment_id,
            amount_local,
            currency,
            start_to_close_timeout=timedelta(seconds=15),
        )
        self.status = PaymentStatus.COMPLETED
        return {"provider_ref": ref, "amount": amount_local, "currency": currency}


@workflow.defn
class StablecoinPaymentWorkflow:
    def __init__(self) -> None:
        self.payment_id: int | None = None
        self.state: dict = {
            "status": PaymentStatus.PENDING.value,
            "legs": {},
            "cancelled": False,
        }

    @workflow.signal
    async def cancel_payment(self) -> None:
        self.state["cancelled"] = True

    @workflow.query
    def get_status(self) -> dict:
        return self.state

    @workflow.run
    async def run(self, request: PaymentRequest) -> dict:
        self.payment_id = await workflow.execute_activity_method(
            StablecoinActivities.initialize_payment,
            request,
            start_to_close_timeout=timedelta(seconds=15),
        )
        pricing = await workflow.execute_activity_method(
            StablecoinActivities.apply_fx_and_fees,
            self.payment_id,
            request,
            start_to_close_timeout=timedelta(seconds=10),
        )

        try:
            collect = await workflow.execute_child_workflow(
                CollectUSDWorkflow.run,
                self.payment_id,
                request.amount_usd,
                id=f"{workflow.info().workflow_id}-collect",
                task_queue=STABLECOIN_TASK_QUEUE_NAME,
            )
            self.state["legs"][PaymentLegType.COLLECT.value] = collect
            await self._ensure_not_cancelled()

            mint = await workflow.execute_child_workflow(
                MintUSDCWorkflow.run,
                self.payment_id,
                pricing["usdc_amount"],
                id=f"{workflow.info().workflow_id}-mint",
                task_queue=STABLECOIN_TASK_QUEUE_NAME,
            )
            self.state["legs"][PaymentLegType.MINT.value] = mint
            await self._ensure_not_cancelled()

            offramp = await workflow.execute_child_workflow(
                OfframpWorkflow.run,
                self.payment_id,
                pricing["local_amount"],
                request.local_currency,
                id=f"{workflow.info().workflow_id}-offramp",
                task_queue=STABLECOIN_TASK_QUEUE_NAME,
            )
            self.state["legs"][PaymentLegType.OFFRAMP.value] = offramp
            await workflow.execute_activity_method(
                StablecoinActivities.mark_payment_complete,
                self.payment_id,
                start_to_close_timeout=timedelta(seconds=10),
            )
            self.state["status"] = PaymentStatus.COMPLETED.value
            return {
                "payment_id": self.payment_id,
                "legs": self.state["legs"],
                "local_amount": pricing["local_amount"],
                "fee": pricing["fee"],
            }
        except Exception as exc:  # pragma: no cover - exercised in tests via failure path
            await self._handle_failure(str(exc))
            raise

    async def _handle_failure(self, reason: str) -> None:
        await workflow.execute_activity_method(
            StablecoinActivities.mark_payment_failed,
            self.payment_id,
            reason,
            start_to_close_timeout=timedelta(seconds=10),
        )
        self.state["status"] = PaymentStatus.FAILED.value

    async def _ensure_not_cancelled(self) -> None:
        if self.state.get("cancelled"):
            await workflow.execute_activity_method(
                StablecoinActivities.mark_payment_cancelled,
                self.payment_id,
                start_to_close_timeout=timedelta(seconds=10),
            )
            await workflow.execute_activity_method(
                StablecoinActivities.flag_for_reconciliation,
                self.payment_id,
                "Cancelled mid-flight",
                start_to_close_timeout=timedelta(seconds=10),
            )
            self.state["status"] = PaymentStatus.CANCELLED.value
            raise RuntimeError("Payment cancelled")


# @@@SNIPEND
