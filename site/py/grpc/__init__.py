"""Browser-only stand-in for grpcio. The generated *_pb2_grpc.py modules `import grpc` at
module level, but a browser has no gRPC; the demo never opens a channel, so only the
names touched at import time (and by the in-memory hand-off's error path) are needed."""

__version__ = "1.84.0"

from . import aio  # noqa: E402,F401


class StatusCode:
    UNAVAILABLE = "UNAVAILABLE"
    UNIMPLEMENTED = "UNIMPLEMENTED"
