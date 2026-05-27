#!/usr/bin/env bash
# Fetch the current ETH validator median APR across 7d / 30d / 90d windows.
# Requires jq.

set -euo pipefail

FEED='https://raw.githubusercontent.com/nocaeth/eth-median-apr-feed/main/data/eth-median-apr.json'

curl -sSf "$FEED" | jq '{
  end_date,
  computed_at,
  median_7d:  .windows["7d"].value_pct,
  median_30d: .windows["30d"].value_pct,
  median_90d: .windows["90d"].value_pct,
}'
