"""Regenerate the sandboxd gRPC client using the protobuf-6-compatible toolchain."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import grpc_tools


ROOT = Path(__file__).resolve().parents[1]
PROTO_ROOT = ROOT / "runtime" / "proto" / "process" / "v1"
OUTPUT_ROOT = ROOT / "src" / "gke_session_sandbox" / "_sandboxd_proto"
GRPC_INCLUDE = Path(grpc_tools.__file__).resolve().parent / "_proto"

subprocess.run(
    [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        "-I",
        str(PROTO_ROOT),
        "-I",
        str(GRPC_INCLUDE),
        f"--python_out={OUTPUT_ROOT}",
        f"--grpc_python_out={OUTPUT_ROOT}",
        "process.proto",
    ],
    cwd=ROOT,
    check=True,
)

grpc_stub = OUTPUT_ROOT / "process_pb2_grpc.py"
contents = grpc_stub.read_text()
generated_import = "import process_pb2 as process__pb2"
if contents.count(generated_import) != 1:
    raise RuntimeError("unexpected grpcio-tools output: process_pb2 import was not found exactly once")
grpc_stub.write_text(contents.replace(generated_import, "from . import process_pb2 as process__pb2"))
