# @@@SNIPSTART python-money-transfer-project-template-run-worker
import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from client_provider import get_temporal_client
from activities import BankingActivities, StablecoinActivities
from shared import MONEY_TRANSFER_TASK_QUEUE_NAME, STABLECOIN_TASK_QUEUE_NAME
from workflows import (
    CollectUSDWorkflow,
    MintUSDCWorkflow,
    MoneyTransfer,
    OfframpWorkflow,
    StablecoinPaymentWorkflow,
)


async def main() -> None:
    client = await get_temporal_client()
    banking_activities = BankingActivities()
    stablecoin_activities = StablecoinActivities()

    money_transfer_worker: Worker = Worker(
        client,
        task_queue=MONEY_TRANSFER_TASK_QUEUE_NAME,
        workflows=[MoneyTransfer],
        activities=[
            banking_activities.withdraw,
            banking_activities.deposit,
            banking_activities.refund,
        ],
    )

    stablecoin_worker: Worker = Worker(
        client,
        task_queue=STABLECOIN_TASK_QUEUE_NAME,
        workflows=[
            StablecoinPaymentWorkflow,
            CollectUSDWorkflow,
            MintUSDCWorkflow,
            OfframpWorkflow,
        ],
        activities=[
            stablecoin_activities.initialize_payment,
            stablecoin_activities.apply_fx_and_fees,
            stablecoin_activities.collect_usd,
            stablecoin_activities.mint_usdc,
            stablecoin_activities.offramp_local,
            stablecoin_activities.flag_for_reconciliation,
            stablecoin_activities.mark_payment_complete,
            stablecoin_activities.mark_payment_failed,
            stablecoin_activities.mark_payment_cancelled,
        ],
    )

    await asyncio.gather(money_transfer_worker.run(), stablecoin_worker.run())


if __name__ == "__main__":
    asyncio.run(main())
# @@@SNIPEND
