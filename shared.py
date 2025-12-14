# @@@SNIPSTART python-money-transfer-project-template-shared
from dataclasses import dataclass
from enum import Enum
from typing import Optional

MONEY_TRANSFER_TASK_QUEUE_NAME = "TRANSFER_MONEY_TASK_QUEUE"
STABLECOIN_TASK_QUEUE_NAME = "STABLECOIN_TASK_QUEUE"


class PaymentStatus(Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    NEEDS_RECONCILIATION = "needs_reconciliation"


class PaymentLegType(Enum):
    COLLECT = "collect_usd"
    MINT = "mint_usdc"
    OFFRAMP = "offramp_local"


@dataclass
class PaymentDetails:
    source_account: str
    target_account: str
    amount: int
    reference_id: str


@dataclass
class PaymentRequest:
    external_ref: str
    amount_usd: float
    destination: str
    local_currency: str
    fx_rate: float
    requested_local_amount: Optional[float] = None


# @@@SNIPEND
