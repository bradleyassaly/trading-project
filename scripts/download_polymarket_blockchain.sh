#!/bin/bash
# Downloads last N blocks of Polymarket trades via poly-trade-scan.
# Requires: pip install poly-trade-scan
# Requires: POLYGON_WSS_URL set in .env (e.g. wss://polygon.drpc.org)
#
# Usage: ./scripts/download_polymarket_blockchain.sh 500000

set -e

BLOCKS=${1:-100000}
OUTPUT="data/polymarket/raw/blockchain_trades.csv"

mkdir -p "$(dirname "$OUTPUT")"

echo "Downloading last $BLOCKS blocks of Polymarket trades..."
poly download -b "$BLOCKS" -o "$OUTPUT"

echo "Downloaded to $OUTPUT"
echo "Now run: trading-cli data polymarket blockchain-ingest --trades-csv $OUTPUT"
