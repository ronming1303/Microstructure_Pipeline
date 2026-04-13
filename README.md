# Microstructure Pipeline

This repository collects Coinbase BTC order-book imbalance data on an hourly schedule while the workflow is running.

## What it does

- GitHub Actions starts once per hour.
- The job stays alive for about one hour.
- During that run, the script samples Coinbase's public order book once per minute.
- Each sample is written to a minute-level CSV.
- When the run finishes, the script appends one hourly summary row.

## Files written by the collector

- `data/minute/YYYY-MM-DD_btc_usd_d10.csv`: daily minute-level shard files with top 10 levels on each side.
- `data/hourly/YYYY-MM-DD_btc_usd_hourly_d10.csv`: daily hourly shard files.
- `state/collector_state.json`: last run metadata.
- `public/data/imbalance_latest.json`: latest point for homepage cards.
- `public/data/imbalance_hourly_tail.json`: recent hourly time series for homepage chart.

## Imbalance formula

For a given sample, the depth imbalance is:

$$
I_t^{(N)} = \frac{\sum_{i=1}^{N}V_{bid,i}(t)-\sum_{i=1}^{N}V_{ask,i}(t)}{\sum_{i=1}^{N}V_{bid,i}(t)+\sum_{i=1}^{N}V_{ask,i}(t)}
$$

The hourly summary reports mean, median, min, and max across the successful minute samples collected during the run.

## Workflow

The GitHub Actions workflow lives in `.github/workflows/hourly_imbalance.yml`.

If you want a different depth, change the `--depth` value in the workflow and re-run.

Minute and hourly data are automatically sharded by UTC day to keep each CSV manageable.

## Homepage integration

Use these two files directly in your homepage frontend:

- `public/data/imbalance_latest.json`
- `public/data/imbalance_hourly_tail.json`

This keeps the page simple: fetch static JSON and render cards + line chart.

## Caveat

This is an hourly live sampler, not a historical backfill job. It is the simplest way to get minute-level data without running a separate always-on service.