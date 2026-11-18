#!/usr/bin/env bash
# Generate Python gRPC stubs from proto/access.proto into src/idsync.
set -euo pipefail
cd "$(dirname "$0")/.."
python -m grpc_tools.protoc -I proto --python_out=src/idsync --grpc_python_out=src/idsync proto/access.proto
# Generated stubs use absolute imports; make them package-relative.
sed -i.bak 's/^import access_pb2 as/from . import access_pb2 as/' src/idsync/access_pb2_grpc.py && rm src/idsync/access_pb2_grpc.py.bak
