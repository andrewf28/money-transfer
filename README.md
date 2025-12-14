# Temporal Money Transfer Python Project Template

This is the code to support the tutorial at https://learn.temporal.io/getting_started/python/first_program_in_python/

To use this code, make sure you have a [Temporal Cluster running](https://docs.temporal.io/docs/server/quick-install/) first.


Clone this repo and run this application.

```bash
git clone https://github.com/temporalio/money-transfer-project-template-python
cd money-transfer-project-template-python
```

Create a virtual environment and activate it. On macOS and Linux, run these commands:

```
python3 -m venv env
source env/bin/activate
```

On Windows, run these commands:

```
python -m venv env
env\Scripts\activate
```

With the virtual environment configured, install the Temporal SDK:

```
python -m pip install temporalio
```


Run the workflow:

```bash
python run_workflow.py
```

In another window, activate the virtual environment:

On macOS or Linux, run this command:

```
source env/bin/activate
```

On Windows, run this command:

```
env\Scripts\activate
```


Then run the worker:


```bash
python run_worker.py
```

Please [read the tutorial](https://learn.temporal.io/getting_started/python/first_program_in_python/) for more details.

## Stablecoin orchestration demo

This repo now includes a simple USD → USDC → local currency workflow using Temporal child workflows, signals, and queries:

- Parent workflow: `StablecoinPaymentWorkflow` in `workflows.py`
- Child workflows: `CollectUSDWorkflow`, `MintUSDCWorkflow`, `OfframpWorkflow`
- Activities: stateful persistence and pricing logic in `StablecoinActivities` (see `activities.py` and `payment_db.py`)

Run the worker to host both the legacy money-transfer example and the new stablecoin workflow:

```bash
python run_worker.py
```

Then trigger both sample workflows in one shot:

```bash
python run_workflow.py
```

The stablecoin run records its state in `payments.db` (overridable via `PAYMENTS_DB`).
