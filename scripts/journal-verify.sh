#!/usr/bin/env bash
# Compare journal integrity across two servers using BLAKE3 chain hashes.
#
# Builds and runs the journal-verify binary on each server, then
# compares the output. Matching chain_hash + last_seq means the
# journals contain identical event streams.
#
# Usage:
#   ./scripts/journal-verify.sh <server1> <journal1> <server2> <journal2>
#
# Example:
#   ./scripts/journal-verify.sh root@primary /mnt/journal/bench.journal \
#                                root@replica /mnt/journal/replica.journal

set -euo pipefail

if [[ $# -lt 4 ]]; then
    echo "usage: $0 <server1> <journal1> <server2> <journal2>"
    exit 1
fi

SERVER1="$1"
JOURNAL1="$2"
SERVER2="$3"
JOURNAL2="$4"

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
REPO_DIR="~/workspace/melin"

echo "=== Journal Verification ==="
echo ""

# Build the verify tool on both servers (cached — instant if already built).
for HOST in "$SERVER1" "$SERVER2"; do
    ssh $SSH_OPTS "$HOST" "cd ${REPO_DIR} && source ~/.cargo/env && \
        cargo build --release -p melin-server --bin journal-verify 2>&1 | tail -1"
done

echo "  Server 1: ${SERVER1} → ${JOURNAL1}"
OUT1=$(ssh $SSH_OPTS "$SERVER1" "cd ${REPO_DIR} && ./target/release/journal-verify ${JOURNAL1}")
echo "$OUT1" | sed 's/^/    /'
echo ""

echo "  Server 2: ${SERVER2} → ${JOURNAL2}"
OUT2=$(ssh $SSH_OPTS "$SERVER2" "cd ${REPO_DIR} && ./target/release/journal-verify ${JOURNAL2}")
echo "$OUT2" | sed 's/^/    /'
echo ""

# Extract chain hashes and sequences for comparison.
HASH1=$(echo "$OUT1" | grep chain_hash | awk '{print $2}')
HASH2=$(echo "$OUT2" | grep chain_hash | awk '{print $2}')
SEQ1=$(echo "$OUT1" | grep last_seq | awk '{print $2}')
SEQ2=$(echo "$OUT2" | grep last_seq | awk '{print $2}')

if [[ "$HASH1" == "$HASH2" && "$SEQ1" == "$SEQ2" ]]; then
    echo "  MATCH — journals are consistent (seq=${SEQ1}, hash=${HASH1})"
elif [[ "$HASH1" == "$HASH2" ]]; then
    echo "  PARTIAL MATCH — same chain hash but different sequence counts"
    echo "    seq1=${SEQ1} seq2=${SEQ2} (replica may have fewer seed events)"
else
    echo "  MISMATCH — chain hashes differ!"
    echo "    hash1=${HASH1}"
    echo "    hash2=${HASH2}"
    exit 1
fi
