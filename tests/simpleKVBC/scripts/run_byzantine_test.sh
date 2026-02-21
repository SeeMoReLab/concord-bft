#!/bin/bash
#
# Run skvbc with Byzantine fault injection on replica 0.
# Usage: ./run_byzantine_test.sh [fault_config.json] [num_ops]
#
# Metrics are fetched from each replica's UDP metrics server after the test.
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FAULT_CONFIG="${1:-$SCRIPT_DIR/../TesterReplica/strategy/byzantine_fault_config.json}"
NUM_OPS="${2:-1800}"
ADAPTIVE_ITERATIONS="${3:-200}"
LOG_DIR="${SCRIPT_DIR}/logs_$(date +%Y%m%dT%H%M%S)"

# Replica network layout:
#   id  address    consensus-port  client-port  agent-port  data-exchange-port
#   0   127.0.0.1  11000           10000        50000       55000
#   1   127.0.0.1  11001           10001        50001       55001
#   2   127.0.0.1  11002           10002        50002       55002
#   3   127.0.0.1  11003           10003        50003       55003
REPLICA_ADDR="localhost"
AGENT_PORTS=(50000 50001 50002 50003)
DATA_EXCHANGE_PORTS=(55000 55001 55002 55003)

mkdir -p "$LOG_DIR"

echo "=== Byzantine Fault Test ==="
echo "Fault config: $FAULT_CONFIG"
echo "Num operations: $NUM_OPS"
echo "Adaptive iterations: $ADAPTIVE_ITERATIONS"
echo "Log dir: $LOG_DIR"
echo ""

# Cleanup
echo "Killing any leftover replicas/clients..."
killall skvbc_replica skvbc_client 2>/dev/null || true
sleep 1

# Verify learning agents are reachable before starting replicas
echo "Checking learning agents are reachable..."
for id in 0 1 2 3; do
  port="${AGENT_PORTS[$id]}"
  if ! python3 -c "
import socket, sys
s = socket.socket()
s.settimeout(2)
try:
    s.connect(('localhost', $port))
    s.close()
    sys.exit(0)
except Exception as e:
    print(f'Agent {$id} not reachable on port $port: {e}', file=sys.stderr)
    sys.exit(1)
"; then
    echo "ERROR: learning agent $id is not running on ${REPLICA_ADDR}:${port}"
    echo "Start all agents before running this script."
    exit 1
  fi
done
echo "All agents reachable."
echo ""

# Start replicas
# Replica 0: Byzantine (with fault config)
echo "Starting replica 0 (BYZANTINE, agent=${REPLICA_ADDR}:${AGENT_PORTS[0]})..."
../TesterReplica/skvbc_replica -k setA_replica_ -i 0 \
  --byzantine-fault-config "$FAULT_CONFIG" \
  --learning-agent-addr "${REPLICA_ADDR}:${AGENT_PORTS[0]}" \
  --adaptive-timer-iterations "$ADAPTIVE_ITERATIONS" \
  > "$LOG_DIR/replica_0.log" 2>&1 &
PIDS[0]=$!

# Replicas 1-3: Honest
for id in 1 2 3; do
  echo "Starting replica $id (honest, agent=${REPLICA_ADDR}:${AGENT_PORTS[$id]})..."
  ../TesterReplica/skvbc_replica -k setA_replica_ -i $id \
    --learning-agent-addr "${REPLICA_ADDR}:${AGENT_PORTS[$id]}" \
    --adaptive-timer-iterations "$ADAPTIVE_ITERATIONS" \
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
