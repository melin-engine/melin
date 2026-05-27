#!/usr/bin/env bash
# Launch two EC2 instances for Melin benchmarking: one primary server, one bench client.
#
# Prerequisites:
#   - AWS CLI v2 configured (`aws configure` or env vars)
#   - An SSH key pair registered in EC2 (--key-name)
#   - Default VPC with internet access (or specify --subnet-id / --security-group-id)
#
# Usage:
#   ./scripts/aws/launch.sh [options]
#
# Options:
#   --key-name <name>         EC2 key pair name (required)
#   --key <path>              SSH private key file (required — used for setup)
#   --instance-type <type>    Instance type (default: c7i.8xlarge)
#   --ami <id>                AMI ID (default: latest Ubuntu 24.04 x86_64 in region)
#   --region <region>         AWS region (default: from AWS CLI config)
#   --subnet-id <id>         Specific subnet (default: first in default VPC)
#   --security-group-id <id> Existing SG (default: creates melin-bench-sg)
#   --smt                     Keep hyperthreading enabled (default: disabled)
#   --skip-setup              Skip server-setup.sh (just launch raw instances)
#   --user <name>             SSH user (default: ubuntu)
#   --output <path>           Write instance metadata JSON (default: /tmp/melin-aws-instances.json)
#
# Examples:
#   ./scripts/aws/launch.sh --key-name my-key --key ~/.ssh/my-key.pem
#   ./scripts/aws/launch.sh --key-name my-key --key ~/.ssh/my-key.pem --instance-type c7i.4xlarge --smt

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
KEY_NAME=""
SSH_KEY=""
INSTANCE_TYPE="c7i.8xlarge"
AMI_ID=""
REGION=""
SUBNET_ID=""
SECURITY_GROUP_ID=""
DISABLE_SMT=1
SKIP_SETUP=0
SSH_USER="ubuntu"
OUTPUT="/tmp/melin-aws-instances.json"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --key-name)       KEY_NAME="$2"; shift 2 ;;
        --key)            SSH_KEY="$2"; shift 2 ;;
        --instance-type)  INSTANCE_TYPE="$2"; shift 2 ;;
        --ami)            AMI_ID="$2"; shift 2 ;;
        --region)         REGION="$2"; shift 2 ;;
        --subnet-id)     SUBNET_ID="$2"; shift 2 ;;
        --security-group-id) SECURITY_GROUP_ID="$2"; shift 2 ;;
        --smt)            DISABLE_SMT=0; shift ;;
        --skip-setup)     SKIP_SETUP=1; shift ;;
        --user)           SSH_USER="$2"; shift 2 ;;
        --output)         OUTPUT="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,/^$/p' "$0" | sed 's/^# \?//'
            exit 0 ;;
        *) echo "error: unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "$KEY_NAME" ]]; then
    echo "error: --key-name is required" >&2
    exit 1
fi

if [[ -z "$SSH_KEY" ]]; then
    echo "error: --key is required (SSH private key path)" >&2
    exit 1
fi

if [[ ! -f "$SSH_KEY" ]]; then
    echo "error: SSH key not found: $SSH_KEY" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SETUP_SCRIPT="$SCRIPT_DIR/../server-setup.sh"
if [[ "$SKIP_SETUP" -eq 0 && ! -f "$SETUP_SCRIPT" ]]; then
    echo "error: server-setup.sh not found at $SETUP_SCRIPT" >&2
    exit 1
fi

REGION_ARGS=()
if [[ -n "$REGION" ]]; then
    REGION_ARGS=(--region "$REGION")
fi

# Track created resources for cleanup on failure.
CREATED_SG=""
CREATED_INSTANCES=()

cleanup_on_error() {
    local exit_code=$?
    if [[ $exit_code -eq 0 ]]; then
        return
    fi
    echo "" >&2
    echo "=== Launch failed — cleaning up ===" >&2
    if [[ ${#CREATED_INSTANCES[@]} -gt 0 ]]; then
        echo "  Terminating instances: ${CREATED_INSTANCES[*]}" >&2
        aws ec2 terminate-instances "${REGION_ARGS[@]}" \
            --instance-ids "${CREATED_INSTANCES[@]}" >/dev/null 2>&1 || true
    fi
    if [[ -n "$CREATED_SG" ]]; then
        # Wait briefly for instances to release the SG.
        sleep 3
        echo "  Deleting security group: $CREATED_SG" >&2
        aws ec2 delete-security-group "${REGION_ARGS[@]}" \
            --group-id "$CREATED_SG" 2>/dev/null || true
    fi
    rm -f "$OUTPUT"
}
trap cleanup_on_error EXIT

# ---------------------------------------------------------------------------
# Resolve AMI (latest Ubuntu 24.04 x86_64)
# ---------------------------------------------------------------------------
if [[ -z "$AMI_ID" ]]; then
    echo "=== Resolving latest Ubuntu 24.04 AMI ==="
    AMI_ID=$(aws ec2 describe-images "${REGION_ARGS[@]}" \
        --owners 099720109477 \
        --filters \
            "Name=name,Values=ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*" \
            "Name=state,Values=available" \
            "Name=architecture,Values=x86_64" \
        --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' \
        --output text)
    if [[ -z "$AMI_ID" || "$AMI_ID" == "None" ]]; then
        echo "error: could not find Ubuntu 24.04 AMI" >&2
        exit 1
    fi
    echo "  AMI: $AMI_ID"
fi

# ---------------------------------------------------------------------------
# Security group (create if not provided)
# ---------------------------------------------------------------------------
if [[ -z "$SECURITY_GROUP_ID" ]]; then
    echo "=== Creating security group ==="
    # Check if our SG already exists.
    SECURITY_GROUP_ID=$(aws ec2 describe-security-groups "${REGION_ARGS[@]}" \
        --filters "Name=group-name,Values=melin-bench-sg" \
        --query 'SecurityGroups[0].GroupId' \
        --output text 2>/dev/null || true)

    if [[ -z "$SECURITY_GROUP_ID" || "$SECURITY_GROUP_ID" == "None" ]]; then
        SECURITY_GROUP_ID=$(aws ec2 create-security-group "${REGION_ARGS[@]}" \
            --group-name melin-bench-sg \
            --description "Melin benchmark - SSH + internal traffic" \
            --query 'GroupId' --output text)

        # SSH restricted to the caller's public IP.
        MY_IP=$(curl -s --max-time 5 https://checkip.amazonaws.com || true)
        if [[ -n "$MY_IP" ]]; then
            SSH_CIDR="${MY_IP}/32"
        else
            echo "  warning: could not detect public IP, allowing SSH from 0.0.0.0/0" >&2
            SSH_CIDR="0.0.0.0/0"
        fi
        aws ec2 authorize-security-group-ingress "${REGION_ARGS[@]}" \
            --group-id "$SECURITY_GROUP_ID" \
            --protocol tcp --port 22 --cidr "$SSH_CIDR" >/dev/null

        # All traffic within the SG (server ↔ bench).
        aws ec2 authorize-security-group-ingress "${REGION_ARGS[@]}" \
            --group-id "$SECURITY_GROUP_ID" \
            --protocol all --source-group "$SECURITY_GROUP_ID" >/dev/null

        CREATED_SG="$SECURITY_GROUP_ID"
        echo "  Created: $SECURITY_GROUP_ID (melin-bench-sg)"
    else
        echo "  Reusing existing: $SECURITY_GROUP_ID"
    fi
fi

# ---------------------------------------------------------------------------
# CPU options
# ---------------------------------------------------------------------------
CPU_OPTIONS=""
if [[ "$DISABLE_SMT" -eq 1 ]]; then
    # Determine physical core count from instance type.
    VCPUS=$(aws ec2 describe-instance-types "${REGION_ARGS[@]}" \
        --instance-types "$INSTANCE_TYPE" \
        --query 'InstanceTypes[0].VCpuInfo.DefaultVCpus' --output text)
    CORES=$((VCPUS / 2))
    CPU_OPTIONS="CoreCount=${CORES},ThreadsPerCore=1"
    echo "=== SMT disabled: ${CORES} physical cores ==="
fi

# ---------------------------------------------------------------------------
# Launch instances
# ---------------------------------------------------------------------------
echo "=== Launching 2x $INSTANCE_TYPE ==="

LAUNCH_ARGS=(
    "${REGION_ARGS[@]}"
    --image-id "$AMI_ID"
    --instance-type "$INSTANCE_TYPE"
    --key-name "$KEY_NAME"
    --security-group-ids "$SECURITY_GROUP_ID"
    --count 2
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=melin-bench},{Key=melin-role,Value=bench-pair}]"
    --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=30,VolumeType=gp3}"
    --metadata-options "HttpTokens=required,HttpEndpoint=enabled"
)

if [[ -n "$SUBNET_ID" ]]; then
    LAUNCH_ARGS+=(--subnet-id "$SUBNET_ID")
fi

if [[ -n "$CPU_OPTIONS" ]]; then
    LAUNCH_ARGS+=(--cpu-options "$CPU_OPTIONS")
fi

INSTANCE_IDS=$(aws ec2 run-instances "${LAUNCH_ARGS[@]}" \
    --query 'Instances[*].InstanceId' --output text)

SERVER_ID=$(echo "$INSTANCE_IDS" | awk '{print $1}')
BENCH_ID=$(echo "$INSTANCE_IDS" | awk '{print $2}')

if [[ -z "$SERVER_ID" || -z "$BENCH_ID" ]]; then
    echo "error: expected 2 instance IDs, got: $INSTANCE_IDS" >&2
    exit 1
fi

CREATED_INSTANCES=("$SERVER_ID" "$BENCH_ID")

# Tag with roles.
aws ec2 create-tags "${REGION_ARGS[@]}" --resources "$SERVER_ID" \
    --tags Key=Name,Value=melin-server Key=melin-role,Value=server >/dev/null
aws ec2 create-tags "${REGION_ARGS[@]}" --resources "$BENCH_ID" \
    --tags Key=Name,Value=melin-bench-client Key=melin-role,Value=bench >/dev/null

echo "  Server: $SERVER_ID"
echo "  Bench:  $BENCH_ID"

# ---------------------------------------------------------------------------
# Wait for running + status checks
# ---------------------------------------------------------------------------
echo "=== Waiting for instances to pass status checks ==="
aws ec2 wait instance-status-ok "${REGION_ARGS[@]}" --instance-ids "$SERVER_ID" "$BENCH_ID"
echo "  Both instances ready."

# ---------------------------------------------------------------------------
# Collect IPs
# ---------------------------------------------------------------------------
read -r SERVER_PUB SERVER_PRIV <<< "$(aws ec2 describe-instances "${REGION_ARGS[@]}" \
    --instance-ids "$SERVER_ID" \
    --query 'Reservations[0].Instances[0].[PublicIpAddress,PrivateIpAddress]' \
    --output text)"

read -r BENCH_PUB BENCH_PRIV <<< "$(aws ec2 describe-instances "${REGION_ARGS[@]}" \
    --instance-ids "$BENCH_ID" \
    --query 'Reservations[0].Instances[0].[PublicIpAddress,PrivateIpAddress]' \
    --output text)"

echo ""
echo "=== Instance details ==="
echo "  Server: $SERVER_ID  pub=$SERVER_PUB  priv=$SERVER_PRIV"
echo "  Bench:  $BENCH_ID  pub=$BENCH_PUB  priv=$BENCH_PRIV"

# ---------------------------------------------------------------------------
# Write metadata
# ---------------------------------------------------------------------------
EFFECTIVE_REGION="${REGION:-$(aws configure get region 2>/dev/null || echo "")}"
SMT_DISABLED=$( [[ "$DISABLE_SMT" -eq 1 ]] && echo "true" || echo "false" )

jq -n \
    --arg region         "$EFFECTIVE_REGION" \
    --arg instance_type  "$INSTANCE_TYPE" \
    --arg ami_id         "$AMI_ID" \
    --argjson smt_disabled "$SMT_DISABLED" \
    --arg sg_id          "$SECURITY_GROUP_ID" \
    --arg server_id      "$SERVER_ID" \
    --arg server_pub     "$SERVER_PUB" \
    --arg server_priv    "$SERVER_PRIV" \
    --arg bench_id       "$BENCH_ID" \
    --arg bench_pub      "$BENCH_PUB" \
    --arg bench_priv     "$BENCH_PRIV" \
    '{
      region: $region,
      instance_type: $instance_type,
      ami_id: $ami_id,
      smt_disabled: $smt_disabled,
      security_group_id: $sg_id,
      server: {instance_id: $server_id, public_ip: $server_pub, private_ip: $server_priv},
      bench:  {instance_id: $bench_id,  public_ip: $bench_pub,  private_ip: $bench_priv}
    }' > "$OUTPUT"

# Metadata written — switch to terminate.sh for cleanup on failure.
cleanup_after_metadata() {
    local exit_code=$?
    if [[ $exit_code -eq 0 ]]; then
        return
    fi
    echo "" >&2
    echo "=== Setup failed — tearing down instances ===" >&2
    "$SCRIPT_DIR/terminate.sh" --instances "$OUTPUT" --yes 2>&1 | sed 's/^/  /' >&2
}
trap cleanup_after_metadata EXIT
echo ""
echo "=== Metadata written to $OUTPUT ==="

# ---------------------------------------------------------------------------
# System setup (server-setup.sh on both instances in parallel)
# ---------------------------------------------------------------------------
if [[ "$SKIP_SETUP" -eq 1 ]]; then
    trap - EXIT
    echo ""
    echo "=== Skipping setup (--skip-setup) ==="
    echo ""
    echo "Run benchmarks:"
    echo "  ./scripts/lan-bench-suite.sh $SERVER_PUB $BENCH_PUB $SERVER_PRIV $SSH_USER"
    echo ""
    echo "Tear down:"
    echo "  ./scripts/aws/terminate.sh --instances $OUTPUT"
    exit 0
fi

echo ""
echo "=== Running server-setup.sh on both instances ==="

SSH_BASE=(-i "$SSH_KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR)
SSH_OPTS=("${SSH_BASE[@]}" -o ConnectTimeout=30)
SSH_QUICK=("${SSH_BASE[@]}" -o ConnectTimeout=5)

setup_host() {
    local pub_ip="$1"
    local role="$2"
    local reboot_flag="$3"

    echo "  [$role] Copying setup script..."
    scp "${SSH_OPTS[@]}" "$SETUP_SCRIPT" "$SSH_USER@$pub_ip:/tmp/server-setup.sh"

    echo "  [$role] Running setup (this takes a few minutes)..."
    ssh "${SSH_OPTS[@]}" "$SSH_USER@$pub_ip" "sudo bash /tmp/server-setup.sh"

    # Check if reboot is needed (touching a local flag file avoids conflating
    # setup errors with reboot requests via exit code).
    if ssh "${SSH_OPTS[@]}" "$SSH_USER@$pub_ip" "test -f /tmp/.server-needs-reboot" 2>/dev/null; then
        echo "  [$role] Rebooting for kernel params..."
        ssh "${SSH_OPTS[@]}" "$SSH_USER@$pub_ip" "sudo reboot" || true
        touch "$reboot_flag"
    fi
}

REBOOT_FLAG_SERVER=$(mktemp)
REBOOT_FLAG_BENCH=$(mktemp)
rm "$REBOOT_FLAG_SERVER" "$REBOOT_FLAG_BENCH"

# Run setup on both in parallel.
setup_host "$SERVER_PUB" "server" "$REBOOT_FLAG_SERVER" &
PID_SERVER=$!

setup_host "$BENCH_PUB" "bench" "$REBOOT_FLAG_BENCH" &
PID_BENCH=$!

SETUP_FAILED=0
wait $PID_SERVER || SETUP_FAILED=1
wait $PID_BENCH  || SETUP_FAILED=1

if [[ "$SETUP_FAILED" -eq 1 ]]; then
    echo "error: setup failed on one or more instances" >&2
    exit 1
fi

NEEDS_REBOOT_SERVER=0
NEEDS_REBOOT_BENCH=0
[[ -f "$REBOOT_FLAG_SERVER" ]] && NEEDS_REBOOT_SERVER=1
[[ -f "$REBOOT_FLAG_BENCH"  ]] && NEEDS_REBOOT_BENCH=1
rm -f "$REBOOT_FLAG_SERVER" "$REBOOT_FLAG_BENCH"

# ---------------------------------------------------------------------------
# Reboot and wait (if needed)
# ---------------------------------------------------------------------------
if [[ "$NEEDS_REBOOT_SERVER" -eq 1 || "$NEEDS_REBOOT_BENCH" -eq 1 ]]; then
    echo ""
    echo "=== Waiting for instances to come back after reboot ==="

    # AWS reboot changes instance state; wait for status checks again.
    sleep 10
    aws ec2 wait instance-status-ok "${REGION_ARGS[@]}" --instance-ids "$SERVER_ID" "$BENCH_ID"

    # Verify SSH is back.
    for host in "$SERVER_PUB" "$BENCH_PUB"; do
        connected=0
        for attempt in $(seq 1 30); do
            if ssh "${SSH_QUICK[@]}" "$SSH_USER@$host" "true" 2>/dev/null; then
                connected=1
                break
            fi
            sleep 2
        done
        if [[ "$connected" -eq 0 ]]; then
            echo "error: SSH did not come back on $host after reboot" >&2
            exit 1
        fi
    done
    echo "  Both instances back online."
fi

# ---------------------------------------------------------------------------
# Verify kernel tuning
# ---------------------------------------------------------------------------
echo ""
echo "=== Verifying kernel tuning ==="
for pair in "$SERVER_PUB:server" "$BENCH_PUB:bench"; do
    host="${pair%%:*}"
    role="${pair##*:}"
    isolated=$(ssh "${SSH_OPTS[@]}" "$SSH_USER@$host" "cat /sys/devices/system/cpu/isolated 2>/dev/null || echo none")
    echo "  [$role] isolcpus=$isolated"
done

trap - EXIT
echo ""
echo "=== Instances ready ==="
echo ""
echo "Run benchmarks:"
echo "  ./scripts/lan-bench-suite.sh $SERVER_PUB $BENCH_PUB $SERVER_PRIV $SSH_USER"
echo ""
echo "Tear down:"
echo "  ./scripts/aws/terminate.sh --instances $OUTPUT"
echo ""
