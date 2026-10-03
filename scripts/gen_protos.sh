#!/bin/sh
# Regenerates src/dssd/<name>pb/ from proto/<name>.proto.
set -eu
cd "$(dirname "$0")/.."
for proto in proto/*.proto; do
  name=$(basename "$proto" .proto)
  out="src/dssd/${name}pb"
  mkdir -p "$out"
  python -m grpc_tools.protoc -I proto --python_out="$out" --pyi_out="$out" --grpc_python_out="$out" "$proto"
  # protoc emits `import x_pb2`; the generated files live in a package.
  sed -i "s/^import ${name}_pb2 as/from . import ${name}_pb2 as/" "$out/${name}_pb2_grpc.py"
done
