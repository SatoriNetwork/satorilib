"""
Satori RPC Module - Direct RPC client for Evermore full nodes

Provides direct RPC access as an alternative to ElectrumX.
Based on ElectrumX battle-tested daemon.py patterns.
"""
from satorilib.rpc.evermore_rpc import (
    EvermoreRpcClient,
    UTXO,
    # Exceptions
    RpcClientError,
    DaemonError,
    WarmingUpError,
    ServiceRefusedError,
    RpcTransportError,
    RpcAuthError,
    RpcMethodNotFound,
    RpcError,
    UtxoQueryError,
    BroadcastRejected,
)

__all__ = [
    'EvermoreRpcClient',
    'UTXO',
    'RpcClientError',
    'DaemonError',
    'WarmingUpError',
    'ServiceRefusedError',
    'RpcTransportError',
    'RpcAuthError',
    'RpcMethodNotFound',
    'RpcError',
    'UtxoQueryError',
    'BroadcastRejected',
]
