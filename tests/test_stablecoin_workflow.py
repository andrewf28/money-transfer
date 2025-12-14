import os
import tempfile
import uuid

import pytest
from temporalio.client import WorkflowFailureError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from activities import StablecoinActivities
from payment_db import PaymentRepository
from shared import PaymentRequest, PaymentStatus, STABLECOIN_TASK_QUEUE_NAME
from workflows import CollectUSDWorkflow, MintUSDCWorkflow, OfframpWorkflow, StablecoinPaymentWorkflow


def _make_repo(tmpdir: str) -> PaymentRepository:
    db_path = os.path.join(tmpdir, "payments.db")
    return PaymentRepository(db_path=db_path)


@pytest.mark.asyncio
async def test_stablecoin_payment_happy_path() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        repo = _make_repo(tmpdir)
        activities = StablecoinActivities(repo)
        request = PaymentRequest(
            external_ref="ext-1",
            amount_usd=50.0,
            destination="dest-abc",
            local_currency="USD",
            fx_rate=1.0,
        )
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue=STABLECOIN_TASK_QUEUE_NAME,
                workflows=[
                    StablecoinPaymentWorkflow,
                    CollectUSDWorkflow,
                    MintUSDCWorkflow,
                    OfframpWorkflow,
                ],
                activities=[
                    activities.initialize_payment,
                    activities.apply_fx_and_fees,
                    activities.collect_usd,
                    activities.mint_usdc,
                    activities.offramp_local,
                    activities.flag_for_reconciliation,
                    activities.mark_payment_complete,
                    activities.mark_payment_failed,
                ],
            ):
                result = await env.client.execute_workflow(
                    StablecoinPaymentWorkflow.run,
                    request,
                    id=str(uuid.uuid4()),
                    task_queue=STABLECOIN_TASK_QUEUE_NAME,
                )

        snapshot = repo.payment_snapshot(result["payment_id"])
        assert snapshot["payment"]["status"] == PaymentStatus.COMPLETED.value
        assert len(snapshot["legs"]) == 3
        assert result["local_amount"] == 49.88  # 0.25% fee applied


@pytest.mark.asyncio
async def test_stablecoin_payment_cancelled() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        repo = _make_repo(tmpdir)
        activities = StablecoinActivities(repo)
        request = PaymentRequest(
            external_ref="ext-2",
            amount_usd=20.0,
            destination="dest-xyz",
            local_currency="USD",
            fx_rate=1.0,
        )
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue=STABLECOIN_TASK_QUEUE_NAME,
                workflows=[
                    StablecoinPaymentWorkflow,
                    CollectUSDWorkflow,
                    MintUSDCWorkflow,
                    OfframpWorkflow,
                ],
                activities=[
                    activities.initialize_payment,
                    activities.apply_fx_and_fees,
                    activities.collect_usd,
                    activities.mint_usdc,
                    activities.offramp_local,
                    activities.flag_for_reconciliation,
                    activities.mark_payment_complete,
                    activities.mark_payment_failed,
                ],
            ):
                handle = await env.client.start_workflow(
                    StablecoinPaymentWorkflow.run,
                    request,
                    id=str(uuid.uuid4()),
                    task_queue=STABLECOIN_TASK_QUEUE_NAME,
                )
                await handle.signal(StablecoinPaymentWorkflow.cancel_payment)
                with pytest.raises(WorkflowFailureError):
                    await handle.result()

        payment_id = repo.ensure_payment(request)
        snapshot = repo.payment_snapshot(payment_id)
        assert snapshot["payment"]["status"] == PaymentStatus.CANCELLED.value
        assert any("reconciliation" in evt["message"] for evt in snapshot["events"])
