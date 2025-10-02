"""
NATS Metal implementation using the C NATS client.
"""
import ctypes
import ctypes.util
import os
from pathlib import Path

# Find the nats.c library
_lib_path = Path(__file__).parent.parent / "nats.c" / "build" / "lib" / "libnats.dylib"
if not _lib_path.exists():
    raise ImportError(f"NATS C library not found at {_lib_path}")

# Load the NATS C library
_libnats = ctypes.CDLL(str(_lib_path))

# Define C types
natsStatus = ctypes.c_int
natsConnection = ctypes.c_void_p

# Define C function signatures
_libnats.natsConnection_ConnectTo.argtypes = [ctypes.POINTER(natsConnection), ctypes.c_char_p]
_libnats.natsConnection_ConnectTo.restype = natsStatus

_libnats.natsConnection_Publish.argtypes = [natsConnection, ctypes.c_char_p, ctypes.c_void_p, ctypes.c_int]
_libnats.natsConnection_Publish.restype = natsStatus

_libnats.natsConnection_IsClosed.argtypes = [natsConnection]
_libnats.natsConnection_IsClosed.restype = ctypes.c_bool

_libnats.natsConnection_Destroy.argtypes = [natsConnection]
_libnats.natsConnection_Destroy.restype = None

# NATS status codes
NATS_OK = 0

class NATSError(Exception):
    pass

class NATS:
    def __init__(self, nc_ptr):
        self._nc = natsConnection(nc_ptr)
    
    @classmethod 
    def connect(cls, url):
        if isinstance(url, str):
            url = url.encode('utf-8')
        
        nc_ptr = natsConnection()
        status = _libnats.natsConnection_ConnectTo(ctypes.byref(nc_ptr), url)
        
        if status != NATS_OK:
            raise NATSError(f"Failed to connect to NATS server: {status}")
            
        return cls(nc_ptr.value)
    
    def publish(self, subject, payload):
        if isinstance(subject, str):
            subject = subject.encode('utf-8')
        # if isinstance(payload, str):
        #     payload = payload.encode('utf-8')
            
        if self._nc is None:
            raise NATSError("Connection is closed")
            
        status = _libnats.natsConnection_Publish(
            self._nc, 
            subject, 
            payload, 
            len(payload)
        )
        
        if status != NATS_OK:
            raise NATSError(f"Failed to publish message: {status}")
    
    def close(self):
        if self._nc is not None:
            _libnats.natsConnection_Destroy(self._nc)
            self._nc = None
