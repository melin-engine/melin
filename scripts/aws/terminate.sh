#!/usr/bin/env bash
# Terminate AWS instances launched by launch.sh and optionally clean up the security group.
#
# Usage:
#   ./scripts/aws/terminate.sh [options]
#
# Options:
#   --instances <path>   Instance metadata JSON (default: /tmp/melin-aws-instances.json)
#   --region <region>    AWS region (default: from AWS CLI config)
#   --keep-sg            Don't delete the security group
#   --yes                Skip confirmation prompt
#
# Examples:
#   ./scripts/aws/terminate.sh
#   ./scripts/aws/terminate.sh --yes --keep-sg

set -euo pipefail

for cmd in aws jq; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "error: $cmd is required but not installed" >&2
        exit 1
    fi
done

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
INSTANCES_FILE="/tmp/melin-aws-instances.json"
REGION=""
KEEP_SG=0
SKIP_CONFIRM=0

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --instances)  INSTANCES_FILE="$2"; shift 2 ;;
        --region)     REGION="$2"; shift 2 ;;
        --keep-sg)    KEEP_SG=1; shift ;;
        --yes)        SKIP_CONFIRM=1; shift ;;
        -h|--help)
            sed -n '2,/^$/p' "$0" | sed 's/^# \?//'
            exit 0 ;;
        *) echo "error: unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ ! -f "$INSTANCES_FILE" ]]; then
    echo "error: instances file not found: $INSTANCES_FILE" >&2
    echo "  Nothing to terminate." >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Read metadata
# ---------------------------------------------------------------------------
SERVER_ID=$(jq -r '.server.instance_id' "$INSTANCES_FILE")
BENCH_ID=$(jq -r '.bench.instance_id' "$INSTANCES_FILE")
SG_ID=$(jq -r '.security_group_id' "$INSTANCES_FILE")
INSTANCE_TYPE=$(jq -r '.instance_type' "$INSTANCES_FILE")
SERVER_ENI=$(jq -r '.dpdk.server_eni_id // empty' "$INSTANCES_FILE" 2>/dev/null || true)
BENCH_ENI=$(jq -r '.dpdk.bench_eni_id // empty' "$INSTANCES_FILE" 2>/dev/null || true)
JOURNAL_VOL=$(jq -r '.journal_volume.volume_id // empty' "$INSTANCES_FILE" 2>/dev/null || true)

# Use region from CLI flag, else from metadata, else from AWS config.
if [[ -z "$REGION" ]]; then
    REGION=$(jq -r '.region // empty' "$INSTANCES_FILE" 2>/dev/null || true)
fi
REGION_ARGS=()
if [[ -n "$REGION" ]]; then
    REGION_ARGS=(--region "$REGION")
fi

echo "=== Melin AWS Teardown ==="
echo "  Instance type: $INSTANCE_TYPE"
echo "  Server: $SERVER_ID"
echo "  Bench:  $BENCH_ID"
echo "  SG:     $SG_ID"
if [[ -n "$SERVER_ENI" ]]; then
    echo "  DPDK ENIs: $SERVER_ENI, $BENCH_ENI"
fi
if [[ -n "$JOURNAL_VOL" ]]; then
    echo "  Journal vol: $JOURNAL_VOL"
fi
echo ""

# ---------------------------------------------------------------------------
# Confirm
# ---------------------------------------------------------------------------
if [[ "$SKIP_CONFIRM" -eq 0 ]]; then
    read -rp "Terminate these instances? [y/N] " confirm
    if [[ "$confirm" != [yY] ]]; then
        echo "Aborted."
        exit 0
    fi
fi

# ---------------------------------------------------------------------------
# Terminate instances
# ---------------------------------------------------------------------------
echo "=== Terminating instances ==="
aws ec2 terminate-instances "${REGION_ARGS[@]}" \
    --instance-ids "$SERVER_ID" "$BENCH_ID" \
    --query 'TerminatingInstances[*].[InstanceId,CurrentState.Name]' \
    --output table

echo "  Waiting for termination..."
aws ec2 wait instance-terminated "${REGION_ARGS[@]}" --instance-ids "$SERVER_ID" "$BENCH_ID"
echo "  Instances terminated."

# ---------------------------------------------------------------------------
# Delete DPDK ENIs (safety net — normally auto-deleted with instance)
# ---------------------------------------------------------------------------
for eni_id in "$SERVER_ENI" "$BENCH_ENI"; do
    if [[ -z "$eni_id" ]]; then continue; fi
    if aws ec2 delete-network-interface "${REGION_ARGS[@]}" \
            --network-interface-id "$eni_id" 2>/dev/null; then
        echo "  Deleted orphaned ENI: $eni_id"
    fi
done

# ---------------------------------------------------------------------------
# Delete journal volume (safety net — normally auto-deleted with instance)
# ---------------------------------------------------------------------------
if [[ -n "$JOURNAL_VOL" ]]; then
    if aws ec2 delete-volume "${REGION_ARGS[@]}" \
            --volume-id "$JOURNAL_VOL" 2>/dev/null; then
        echo "  Deleted orphaned journal volume: $JOURNAL_VOL"
    fi
fi

# ---------------------------------------------------------------------------
# Delete security group
# ---------------------------------------------------------------------------
if [[ "$KEEP_SG" -eq 0 && -n "$SG_ID" && "$SG_ID" != "null" ]]; then
    echo "=== Deleting security group ==="
    if aws ec2 delete-security-group "${REGION_ARGS[@]}" --group-id "$SG_ID" 2>/dev/null; then
        echo "  Deleted: $SG_ID"
    else
        echo "  Could not delete $SG_ID (may be in use by other instances)"
    fi
fi

# ---------------------------------------------------------------------------
# Clean up metadata file
# ---------------------------------------------------------------------------
rm -f "$INSTANCES_FILE"
echo ""
echo "=== Teardown complete ==="
