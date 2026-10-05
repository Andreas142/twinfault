{% docs __overview__ %}
# twinfault analytics

Daily marts built from every USDT, USDC and PYUSD transfer on Ethereum since January 2024,
for the [twinfault website](../index.html). The source tables live in a public
[Hugging Face dataset](https://huggingface.co/datasets/andrew142/stablecoin-payments-eth).

## How a month is built

Each month is built on its own, by a GitHub Actions job:

1. **Sources:** the month's files are downloaded from Hugging Face (one folder per day).
2. **Staging:** one table per source, with types, units and the month filter applied.
3. **Marts:** three daily tables, written as Parquet and uploaded back to Hugging Face:
   - `fct_daily_payments`: the cube, one row per day, token, transfer size and route
   - `fct_daily_token_health`: direct calls, failures and fees per day and token
   - `fct_daily_pipeline`: block completeness and load delay per day

A month is uploaded only if every test passes, including `assert_payments_reconcile`,
which checks that the cube adds up to the raw transfers exactly, day by day.

Click the blue button at the bottom right to see the lineage graph.
{% enddocs %}
