#!/bin/bash
#
# Run skvbc with Byzantine fault injection on replica 0.
# Usage: ./run_byzantine_test.sh [fault_config.json] [num_ops]
#
# Metrics are fetched from each replica's UDP metrics server after the test.
#

#cd build

# 1. Generate key files
#./tools/GenerateConcordKeys -n 4 -f 1 -r 0 -o tests/simpleKVBC/scripts/setA_replica_

# 2. Generate TLS certs
#cd tests/simpleKVBC/scripts
# concord-bft/scripts/linux/create_tls_certs.sh 5

# 3. Clean any stale state
#rm -rf simpleKVBTests_DB_*

# 4. cd /concord-bft
# make login

# 5. Run the test
# cd concord-bft/build/tests/simpleKVBC/scripts
# concord-bft/tests/simpleKVBC/scripts/run_byzantine_test.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FAULT_CONFIG="${1:-$SCRIPT_DIR/../TesterReplica/strategy/byzantine_fault_config.json}"
NUM_OPS="${2:-1800}"
LOG_DIR="${SCRIPT_DIR}/logs_$(date +%Y%m%dT%H%M%S)"

mkdir -p "$LOG_DIR"

echo "=== Byzantine Fault Test ==="
echo "Fault config: $FAULT_CONFIG"
echo "Num operations: $NUM_OPS"
echo "Log dir: $LOG_DIR"
echo ""

# Cleanup
echo "Killing any leftover replicas/clients..."
killall skvbc_replica skvbc_client 2>/dev/null || true
sleep 1

# Start replicas
# Replica 0: Byzantine (with fault config)
echo "Starting replica 0 (BYZANTINE)..."
../TesterReplica/skvbc_replica -k setA_replica_ -i 0 \
  --byzantine-fault-config "$FAULT_CONFIG" \
  > "$LOG_DIR/replica_0.log" 2>&1 &
PIDS[0]=$!

# Replicas 1-3: Honest
for id in 1 2 3; do
  echo "Starting replica $id (honest)..."
  ../TesterReplica/skvbc_replica -k setA_replica_ -i $id \
    > "$LOG_DIR/replica_${id}.log" 2>&1 &
  PIDS[$id]=$!
done

echo "Waiting for replicas to start..."
sleep 3

# Run client
echo "Starting client (id=4, f=1, c=0, ops=$NUM_OPS)..."
START_TIME=$(date +%s%N)

../TesterClient/skvbc_client -f 1 -c 0 -p "$NUM_OPS" -i 4 \
  > "$LOG_DIR/client.log" 2>&1
CLIENT_EXIT=$?

END_TIME=$(date +%s%N)
ELAPSED_MS=$(( (END_TIME - START_TIME) / 1000000 ))

echo ""
echo "=== Results ==="
echo "Client exit code: $CLIENT_EXIT"
echo "Total time: ${ELAPSED_MS} ms"
echo "Operations: $NUM_OPS"
if [ $ELAPSED_MS -gt 0 ]; then
  THROUGHPUT=$(( NUM_OPS * 1000 / ELAPSED_MS ))
  echo "Throughput: ~${THROUGHPUT} ops/sec"
fi
echo ""

# Fetch metrics from each replica via UDP metrics server
# Metrics port = 3710 + (2 * replicaId) + 1000
echo "=== Replica Metrics ==="
for id in 0 1 2 3; do
  METRICS_PORT=$(( 3710 + 2 * id + 1000 ))
  METRICS_FILE="$LOG_DIR/metrics_replica_${id}.json"
  # Send a metrics request (1-byte type=0 + 8-byte seqnum) via UDP
  python3 -c "
import socket, struct, json, sys
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.settimeout(2)
req = struct.pack('<BQ', 0, 1)
try:
    sock.sendto(req, ('127.0.0.1', $METRICS_PORT))
    data, _ = sock.recvfrom(65536)
    header_size = struct.calcsize('<BQ')
    json_str = data[header_size:].decode('utf-8')
    metrics = json.loads(json_str)
    with open('$METRICS_FILE', 'w') as f:
        json.dump(metrics, f, indent=2)
    # Print key consensus metrics
    for comp in metrics.get('Components', []):
        if comp['Name'] == 'replica':
            counters = comp.get('Counters', {})
            gauges = comp.get('Gauges', {})
            print(f'Replica {$id}:')
            for key in ['receivedPrePrepareMsgs', 'sentPrePrepareMsgs',
                        'receivedCommitFullMsgs', 'receivedPartialCommitProofMsgs',
                        'receivedFullCommitProofMsgs', 'slowPathCount', 'fastPathCount']:
                if key in counters:
                    print(f'  {key}: {counters[key]}')
                elif key in gauges:
                    print(f'  {key}: {gauges[key]}')
            for key in ['lastExecutedSeqNum', 'lastAgreedView']:
                if key in gauges:
                    print(f'  {key}: {gauges[key]}')
            break
except socket.timeout:
    print(f'Replica {$id}: metrics timeout (port $METRICS_PORT)')
except Exception as e:
    print(f'Replica {$id}: metrics error: {e}')
finally:
    sock.close()
" 2>&1
done

echo ""
echo "=== Cleanup ==="
killall skvbc_replica 2>/dev/null || true

echo ""
echo "Logs saved to: $LOG_DIR"
echo "  replica_N.log   - replica stdout/stderr"
echo "  client.log      - client output"
echo "  metrics_replica_N.json - full metrics JSON"
