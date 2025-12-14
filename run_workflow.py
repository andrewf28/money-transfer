# @@@SNIPSTART python-project-template-run-workflow
import asyncio
import traceback

from temporalio.client import Client, WorkflowFailureError

from client_provider import get_temporal_client
from shared import (
    MONEY_TRANSFER_TASK_QUEUE_NAME,
    PaymentDetails,
    PaymentRequest,
    STABLECOIN_TASK_QUEUE_NAME,
)
from workflows import MoneyTransfer, StablecoinPaymentWorkflow


async def main() -> None:
    client = await get_temporal_client()

    money_transfer = PaymentDetails(
        source_account="85-150",
        target_account="43-812",
        amount=250,
        reference_id="12345",
    )
    stablecoin = PaymentRequest(
        external_ref="stable-123",
        amount_usd=100.0,
        destination="acct-xyz",
        local_currency="MXN",
        fx_rate=17.25,
    )

    try:
        legacy_result = await client.execute_workflow(
            MoneyTransfer.run,
            money_transfer,
            id="pay-invoice-701",
            task_queue=MONEY_TRANSFER_TASK_QUEUE_NAME,
        )
        print(f"Legacy transfer result: {legacy_result}")

        stablecoin_result = await client.execute_workflow(
            StablecoinPaymentWorkflow.run,
            stablecoin,
            id="stablecoin-demo-1",
            task_queue=STABLECOIN_TASK_QUEUE_NAME,
        )
        print(f"Stablecoin result: {stablecoin_result}")

    except WorkflowFailureError:
        print("Got expected exception: ", traceback.format_exc())


if __name__ == "__main__":
    asyncio.run(main())
# @@@SNIPEND
