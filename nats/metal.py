"""
NATS Metal implementation with Cython and ctypes fallback.
"""

class NATSError(Exception):
    pass

# Try to import Cython implementation first
try:
    from .cnats import CythonNATS
    _USE_CYTHON = True
    print("Using Cython NATS implementation for maximum performance")
except ImportError:
    _USE_CYTHON = False
    print("Cython not available, falling back to ctypes implementation")
    
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

if _USE_CYTHON:
    # Use Cython implementation
    class NATS:
        def __init__(self, cython_instance):
            self._impl = cython_instance
        
        @classmethod 
        def connect(cls, url):
            return cls(CythonNATS.connect(url))
        
        def publish(self, subject, payload):
            self._impl.publish(subject, payload)
        
        def publish_many(self, subject, payload, count):
            """Efficient batch publishing (Cython only)."""
            self._impl.publish_many(subject, payload, count)
        
        def flush(self):
            """Flush pending messages (Cython only)."""
            self._impl.flush()
        
        def flush_timeout(self, timeout_ms):
            """Flush with timeout (Cython only)."""
            self._impl.flush_timeout(timeout_ms)
        
        def is_closed(self):
            return self._impl.is_closed()
        
        def is_reconnecting(self):
            return self._impl.is_reconnecting()
        
        def close(self):
            self._impl.close()

else:
    # Use ctypes implementation as fallback
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
        
        def publish_many(self, subject, payload, count):
            """Batch publishing fallback (just loops)."""
            for _ in range(count):
                self.publish(subject, payload)
        
        def flush(self):
            """Flush not available in ctypes version."""
            pass
        
        def flush_timeout(self, timeout_ms):
            """Flush with timeout not available in ctypes version."""
            pass
        
        def is_closed(self):
            if self._nc is None:
                return True
            return _libnats.natsConnection_IsClosed(self._nc)
        
        def is_reconnecting(self):
            return False  # Not implemented in ctypes version
        
        def close(self):
            if self._nc is not None:
                _libnats.natsConnection_Destroy(self._nc)
                self._nc = None
