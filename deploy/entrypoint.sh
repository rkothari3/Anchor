#!/bin/sh
# Turns this pod's stable identity (its own hostname, from the
# StatefulSet, and the fixed $PEERS list) into dssd-worker's CLI flags.
# SWIM binds to $POD_IP so peers can reach it over UDP; gRPC peers are
# headless-service DNS names. gRPC binds 0.0.0.0 so kubectl port-forward
# (which connects to localhost inside the pod) works too.
set -eu

id="$(hostname)"
grpc_port=8000
swim_port=8001

peer_args=""
join_addr=""
first=""
for name in $PEERS; do
  if [ -z "$first" ]; then
    first="$name"
  fi
  if [ "$name" != "$id" ]; then
    peer_args="$peer_args --peer=${name}=${name}.${SERVICE_NAME}.${POD_NAMESPACE}.svc.cluster.local:${grpc_port}"
  fi
done

if [ "$id" != "$first" ]; then
  join_addr="${first}.${SERVICE_NAME}.${POD_NAMESPACE}.svc.cluster.local:${swim_port}"
fi

set -- --id="$id" \
  --swim-addr="${POD_IP}:${swim_port}" \
  --grpc-addr="0.0.0.0:${grpc_port}" \
  $peer_args \
  "$@"

if [ -n "$join_addr" ]; then
  set -- "$@" --join="$join_addr"
fi

exec dssd-worker "$@"
