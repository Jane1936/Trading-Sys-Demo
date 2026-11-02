
### Alpha 1h funding-rate collector

Run the collector as a long-lived process; it wakes at **HH:01:00 UTC** each hour and writes `interval=1h` symbols to `data/alpha.db` (override with `ALPHA_DB_PATH` or `--db`):

```bash
python alpha_funding_collector.py
```

For an immediate one-off collection:

```bash
python alpha_funding_collector.py --once
```
