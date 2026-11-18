"""gRPC AccessService: answers "what can this identity access?" from the store."""

from __future__ import annotations

from concurrent import futures

import grpc
from psycopg_pool import ConnectionPool

from . import access_pb2, access_pb2_grpc
from .store import get_access


class AccessService(access_pb2_grpc.AccessServiceServicer):
    def __init__(self, pool: ConnectionPool):
        self.pool = pool

    def GetAccess(self, request, context):
        if not request.identity_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "identity_id is required")
        with self.pool.connection() as conn:
            found = get_access(conn, request.identity_id)
        if found is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"no active identity {request.identity_id}")
        kind, version, ents = found
        return access_pb2.GetAccessResponse(
            identity_id=request.identity_id,
            kind=kind,
            version=version,
            entitlements=[access_pb2.Entitlement(resource=r, permission=p) for r, p in ents],
        )


def serve(pool: ConnectionPool, address: str = "127.0.0.1:50051") -> tuple[grpc.Server, int]:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    access_pb2_grpc.add_AccessServiceServicer_to_server(AccessService(pool), server)
    port = server.add_insecure_port(address)
    if port == 0:
        raise RuntimeError(f"could not bind {address}")
    server.start()
    return server, port
