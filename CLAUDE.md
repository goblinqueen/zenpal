# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

ZenPal integrates with the [Zenmoney](https://zenmoney.ru/) personal finance API to:
- Sync financial data between the Zenmoney cloud and a local `zenmoney.json` snapshot
- Parse OP Bank (Finnish bank) CSV exports and import them as transactions
- Use CatBoost ML to automatically predict transaction tags from historical patterns

## Setup

Copy `config_example.py` to `config.py` and fill in your credentials:
```python
ZEN_API_TOKEN = 'your_token_here'  # from https://zerro.app/token
ZEN_USER = your_user_id
```

## Running

```bash
# Main sync + OP Bank import
python main.py

# Multi-currency CSV conversion utility
python zenpal.py -f input.csv -o output.csv [--append True]

# Interactive analysis and ML training
jupyter notebook run.ipynb
```

## Architecture

**`zenmoney.py`** — Core API layer
- `ZenConnection.sync(diff)` — POSTs incremental diffs to `https://api.zenmoney.ru/v8/diff/` using Bearer auth; tracks server timestamp for subsequent syncs
- `Zenmoney` — Local data model; loads/saves `zenmoney.json`; `apply_diff()` merges API responses; exposes `.transaction`, `.tag`, `.account`, etc.

**`op.py`** — OP Bank CSV parser
- `OPReader` parses Finnish bank CSV format, extracts amount/date/payee/message, and produces Zenmoney transaction dicts

**`main.py`** — Orchestration
- `load_or_sync()` — initializes or refreshes `zenmoney.json` from API
- `get_updates()` — diffs OP CSV against existing transactions to produce a sync payload
- OP account UUIDs (EUR, Family, Money Box) are hardcoded constants

**`run.ipynb`** — Jupyter workflow for Pandas-based analysis and CatBoost tag prediction (train/predict cycle)

**`zenpal.py`** — Standalone CSV converter using `forex_python` for historical currency rates

## Key Dependencies

- `requests` — Zenmoney API calls
- `pandas` — transaction data manipulation
- `catboost` + `scikit-learn` — tag prediction model
- `forex_python` — historical forex rates for CSV conversion

## API Reference

Zenmoney sync protocol: https://github.com/zenmoney/ZenPlugins/wiki/ZenMoney-API
